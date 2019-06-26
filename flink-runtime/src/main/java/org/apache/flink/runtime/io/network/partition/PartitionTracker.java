/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor.ReleaseType;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.partition.PartitionTable;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
public class PartitionTracker {

	private final JobID jobId;

	private final PartitionTable<ResourceID> partitionTable;
	private final Map<ExecutionAttemptID, Map<IntermediateResultPartitionID, InternalPartitionInfo>> partitionInfo;

	private final ShuffleMaster<?> shuffleMaster;
	private final boolean forcePartitionReleaseOnConsumption;

	private Function<ResourceID, Optional<TaskExecutorGateway>> taskExecutorGatewayFunction;

	public PartitionTracker(
			JobID jobId,
			ShuffleMaster<?> shuffleMaster,
			boolean forcePartitionReleaseOnConsumption) {
		this(jobId, shuffleMaster, forcePartitionReleaseOnConsumption, null);
	}

	@VisibleForTesting
	public PartitionTracker(
			JobID jobId,
			ShuffleMaster<?> shuffleMaster,
			boolean forcePartitionReleaseOnConsumption,
			Function<ResourceID, Optional<TaskExecutorGateway>> taskExecutorGatewayFunction) {
		this.jobId = Preconditions.checkNotNull(jobId);
		this.shuffleMaster = Preconditions.checkNotNull(shuffleMaster);
		this.forcePartitionReleaseOnConsumption = forcePartitionReleaseOnConsumption;
		this.taskExecutorGatewayFunction = taskExecutorGatewayFunction;
		this.partitionTable = new PartitionTable<>();
		this.partitionInfo = new HashMap<>(10);
	}

	public CompletableFuture<Void> registerProducedPartitions(
			Collection<IntermediateResultPartition> partitions,
			ProducerDescriptor producerDescriptor,
			boolean lazyScheduling,
			Executor mainThreadExecutor) {
		Collection<CompletableFuture<ResultPartitionDeploymentDescriptor>> partitionRegistrations =
			new ArrayList<>(partitions.size());

		for (IntermediateResultPartition partition : partitions) {
			PartitionDescriptor partitionDescriptor = PartitionDescriptor.from(partition);
			int maxParallelism = getPartitionMaxParallelism(partition);
			CompletableFuture<? extends ShuffleDescriptor> shuffleDescriptorFuture =
				shuffleMaster.registerPartitionWithProducer(partitionDescriptor, producerDescriptor);

			boolean releasePartitionOnConsumption =
				forcePartitionReleaseOnConsumption
					|| !partitionDescriptor.getPartitionType().isBlocking();
			ReleaseType releaseType = releasePartitionOnConsumption ? ReleaseType.AUTO : ReleaseType.MANUAL;

			CompletableFuture<ResultPartitionDeploymentDescriptor> partitionRegistration = shuffleDescriptorFuture
				.thenApply(shuffleDescriptor -> new ResultPartitionDeploymentDescriptor(
					partitionDescriptor,
					shuffleDescriptor,
					maxParallelism,
					lazyScheduling,
					releaseType));
			partitionRegistrations.add(partitionRegistration);
		}

		return FutureUtils.thenApplyAsyncIfNotDone(
			FutureUtils.combineAll(partitionRegistrations),
			mainThreadExecutor,
			producedPartitions -> {
				producedPartitions.forEach(p -> startTrackingPartition(producerDescriptor.getProducerLocation(), p));
				return null;
			});
	}

	private static int getPartitionMaxParallelism(IntermediateResultPartition partition) {
		// TODO consumers.isEmpty() only exists for test, currently there has to be exactly one consumer in real jobs!
		final List<List<ExecutionEdge>> consumers = partition.getConsumers();
		int maxParallelism = KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM;
		if (!consumers.isEmpty()) {
			List<ExecutionEdge> consumer = consumers.get(0);
			ExecutionJobVertex consumerVertex = consumer.get(0).getTarget().getJobVertex();
			maxParallelism = consumerVertex.getMaxParallelism();
		}
		return maxParallelism;
	}

	/**
	 * Sets the function for retrieving a {@link TaskExecutorGateway} for the given ResourceID.
	 *
	 * <p>Developer note: this is a hack for the circular dependency between the JobMaster and PartitionTracker
	 * the proper solution would be to introduce a separate TaskManagerTable that both use
	 *
	 * @param taskExecutorGatewayFunction
	 */
	public void setTaskExecutorGatewayRetriever(
			Function<ResourceID, Optional<TaskExecutorGateway>> taskExecutorGatewayFunction) {
		Preconditions.checkState(this.taskExecutorGatewayFunction == null);
		Preconditions.checkNotNull(taskExecutorGatewayFunction);
		this.taskExecutorGatewayFunction = taskExecutorGatewayFunction;
	}

	private void ensureInitialized() {
		Preconditions.checkState(this.taskExecutorGatewayFunction != null);
	}

	/**
	 * Starts the tracking of the given partition for the given task executor ID.
	 *
	 * @param producingTaskExecutorId
	 * @param resultPartitionDeploymentDescriptor
	 */
	private void startTrackingPartition(
			ResourceID producingTaskExecutorId,
			ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
		ensureInitialized();

		Preconditions.checkNotNull(producingTaskExecutorId);
		Preconditions.checkNotNull(resultPartitionDeploymentDescriptor);

		final ResultPartitionID resultPartitionId = resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

		InternalPartitionInfo info = new InternalPartitionInfo(producingTaskExecutorId, resultPartitionDeploymentDescriptor);
		partitionInfo.put(resultPartitionId, info);
		partitionTable.startTrackingPartitions(producingTaskExecutorId, Collections.singletonList(resultPartitionId));
	}

	/**
	 * Stops the tracking of all partitions for the given task executor ID, without issuing any release calls.
	 *
	 * @param producingTaskExecutorId
	 */
	public void stopTrackingAllPartitions(ResourceID producingTaskExecutorId) {
		ensureInitialized();

		Preconditions.checkNotNull(producingTaskExecutorId);

		// this is a bit icky since we make 2 calls to pT#stopTrackingPartitions
		final Collection<ResultPartitionID> resultPartitionIds = partitionTable.stopTrackingPartitions(producingTaskExecutorId);

		for (ResultPartitionID resultPartitionId : resultPartitionIds) {
			internalStopTrackingPartition(resultPartitionId);
		}
	}

	/**
	 * Releases the given partitions and stop the tracking of partitions that were released.
	 *
	 * @param resultPartitionIds
	 */
	@VisibleForTesting
	void stopTrackingAndReleasePartitions(Collection<ResultPartitionID> resultPartitionIds) {
		ensureInitialized();

		Preconditions.checkNotNull(resultPartitionIds);

		// stop tracking partitions to be released and group them by task executor ID
		Map<ResourceID, List<ResultPartitionDeploymentDescriptor>> partitionsToReleaseByResourceId = resultPartitionIds.stream()
			.map(this::internalStopTrackingPartition)
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.groupingBy(
				partitionMetaData -> partitionMetaData.location,
				Collectors.mapping(
					partitionMetaData -> partitionMetaData.descriptor,
					toList())));

		partitionsToReleaseByResourceId.forEach(this::internalReleasePartitions);
	}

	/**
	 * Releases all partitions for the given task executor ID, and stop the tracking of partitions that were released.
	 *
	 * @param producingTaskExecutorId
	 */
	public void stopTrackingAndReleaseAllPartitions(ResourceID producingTaskExecutorId) {
		ensureInitialized();

		Preconditions.checkNotNull(producingTaskExecutorId);

		// this is a bit icky since we make 2 calls to pT#stopTrackingPartitions
		Collection<ResultPartitionID> resultPartitionIds = partitionTable.stopTrackingPartitions(producingTaskExecutorId);

		stopTrackingAndReleasePartitions(resultPartitionIds);
	}

	/**
	 * Returns whether any partition is being tracked for the given task executor ID.
	 *
	 * @param producingTaskExecutorId
	 */
	public boolean isTrackingPartitionsFor(ResourceID producingTaskExecutorId) {
		ensureInitialized();

		Preconditions.checkNotNull(producingTaskExecutorId);

		return partitionTable.hasTrackedPartitions(producingTaskExecutorId);
	}

	public Collection<ResultPartitionDeploymentDescriptor> getResultPartitionDeploymentDescriptors(ExecutionAttemptID executionAttemptID) {
		return partitionInfo.get(executionAttemptID).values().stream().map(i -> i.descriptor).collect(Collectors.toList());
	}

	private Optional<InternalPartitionInfo> internalStopTrackingPartition(ResultPartitionID resultPartitionId) {
		final InternalPartitionInfo info = partitionInfo.remove(resultPartitionId);
		if (info == null) {
			return Optional.empty();
		}
		partitionTable.stopTrackingPartitions(info.location, Collections.singletonList(resultPartitionId));
		return Optional.of(info);
	}

	private void internalReleasePartitions(
			ResourceID potentialPartitionLocation,
			Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {
		Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptorsToRelease =
			partitionDeploymentDescriptors.stream()
				.filter(ResultPartitionDeploymentDescriptor::isReleasedOnConsumption)
				.collect(Collectors.toList());
		internalReleasePartitionsOnTaskExecutor(potentialPartitionLocation, partitionDeploymentDescriptorsToRelease);
		internalReleasePartitionsOnShuffleMaster(partitionDeploymentDescriptorsToRelease);
	}

	private void internalReleasePartitionsOnTaskExecutor(
			ResourceID potentialPartitionLocation,
			Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {

		final List<ResultPartitionID> partitionsRequiringRpcReleaseCalls = partitionDeploymentDescriptors.stream()
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.filter(descriptor -> descriptor.storesLocalResourcesOn().isPresent())
			.map(ShuffleDescriptor::getResultPartitionID)
			.collect(Collectors.toList());

		if (!partitionsRequiringRpcReleaseCalls.isEmpty()) {
			taskExecutorGatewayFunction
				.apply(potentialPartitionLocation)
				.ifPresent(taskExecutorGateway ->
					taskExecutorGateway.releasePartitions(jobId, partitionsRequiringRpcReleaseCalls));
		}
	}

	private void internalReleasePartitionsOnShuffleMaster(
			Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {
		partitionDeploymentDescriptors.stream()
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.forEach(shuffleMaster::releasePartitionExternally);
	}

	private static class InternalPartitionInfo {
		private final ResourceID location;
		private final ResultPartitionDeploymentDescriptor descriptor;

		private InternalPartitionInfo(ResourceID location, ResultPartitionDeploymentDescriptor descriptor) {
			this.location = location;
			this.descriptor = descriptor;
		}
	}
}
