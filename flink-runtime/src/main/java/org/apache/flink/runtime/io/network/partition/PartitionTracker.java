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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.partition.PartitionTable;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
public class PartitionTracker {

	private final JobID jobId;

	private final PartitionTable<ResourceID> partitionTable = new PartitionTable<>();
	private final Map<ResultPartitionID, ResourceID> partitionLocations = new HashMap<>();
	private final Map<ResultPartitionID, ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors = new HashMap<>();

	private final ShuffleMaster<?> shuffleMaster;

	private Function<ResourceID, Optional<TaskExecutorGateway>> taskExecutorGatewayFunction;

	public PartitionTracker(
		JobID jobId,
		ShuffleMaster<?> shuffleMaster) {
		this(jobId, shuffleMaster, null);
	}

	@VisibleForTesting
	public PartitionTracker(
		JobID jobId,
		ShuffleMaster<?> shuffleMaster,
		Function<ResourceID, Optional<TaskExecutorGateway>> taskExecutorGatewayFunction) {

		this.jobId = Preconditions.checkNotNull(jobId);
		this.shuffleMaster = Preconditions.checkNotNull(shuffleMaster);
		this.taskExecutorGatewayFunction = taskExecutorGatewayFunction;
	}

	/**
	 * Sets the function for retrieving a {@link TaskExecutorGateway} for the given ResourceID.
	 *
	 * <p>Developer note: this is a hack for the circular dependency between the JobMaster and PartitionTracker
	 * the proper solution would be to introduce a separate TaskManagerTable that both use
	 *
	 * @param taskExecutorGatewayFunction
	 */
	public void setTaskExecutorGatewayRetriever(final Function<ResourceID, Optional<TaskExecutorGateway>> taskExecutorGatewayFunction) {
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
	public void startTrackingPartition(ResourceID producingTaskExecutorId, ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
		ensureInitialized();

		Preconditions.checkNotNull(producingTaskExecutorId);
		Preconditions.checkNotNull(resultPartitionDeploymentDescriptor);

		// if it is released on consumption we do not need to issue any release calls
		if (resultPartitionDeploymentDescriptor.isReleasedOnConsumption()) {
			return;
		}

		final ResultPartitionID resultPartitionId = resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

		partitionDeploymentDescriptors.put(resultPartitionId, resultPartitionDeploymentDescriptor);
		partitionTable.startTrackingPartitions(producingTaskExecutorId, Collections.singletonList(resultPartitionId));
		partitionLocations.put(resultPartitionId, producingTaskExecutorId);
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
	public void stopTrackingAndReleasePartitions(Collection<ResultPartitionID> resultPartitionIds) {
		ensureInitialized();

		Preconditions.checkNotNull(resultPartitionIds);

		// stop tracking partitions to be released and group them by task executor ID
		Map<ResourceID, List<ResultPartitionDeploymentDescriptor>> partitionsToReleaseByResourceId = resultPartitionIds.stream()
			.map(this::internalStopTrackingPartition)
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.groupingBy(
				partitionMetaData -> partitionMetaData.f0,
				Collectors.mapping(
					partitionMetaData -> partitionMetaData.f1,
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

	private Optional<Tuple2<ResourceID, ResultPartitionDeploymentDescriptor>> internalStopTrackingPartition(ResultPartitionID resultPartitionId) {
		final ResourceID partitionLocation = partitionLocations.remove(resultPartitionId);
		if (partitionLocation == null) {
			return Optional.empty();
		}
		partitionTable.stopTrackingPartitions(partitionLocation, Collections.singletonList(resultPartitionId));
		final ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor = partitionDeploymentDescriptors.remove(resultPartitionId);

		return Optional.of(Tuple2.of(partitionLocation, resultPartitionDeploymentDescriptor));
	}

	private void internalReleasePartitions(
		ResourceID potentialPartitionLocation,
		Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {

		internalReleasePartitionsOnTaskExecutor(potentialPartitionLocation, partitionDeploymentDescriptors);
		internalReleasePartitionsOnShuffleMaster(partitionDeploymentDescriptors);
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

	private void internalReleasePartitionsOnShuffleMaster(Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {
		partitionDeploymentDescriptors.stream()
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.forEach(shuffleMaster::releasePartitionExternally);
	}
}
