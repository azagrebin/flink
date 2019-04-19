/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.deployment;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphException;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.shuffle.ShuffleDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDeploymentDescriptor;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.CheckedSupplier;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Factory of {@link TaskDeploymentDescriptor} to deploy {@link Execution}.
 */
public class TaskDeploymentDescriptorFactory {
	private final ExecutionAttemptID executionId;
	private final int attemptNumber;

	private final TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> serializedJobInformation;
	private final CheckedSupplier<Either<SerializedValue<TaskInformation>, PermanentBlobKey>> taskInfoSupplier;
	private final JobID jobID;
	private final boolean lazyScheduling;
	private final int subtaskIndex;
	private final ExecutionEdge[][] inputEdges;

	private TaskDeploymentDescriptorFactory(
		ExecutionAttemptID executionId,
		int attemptNumber,
		TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> serializedJobInformation,
		CheckedSupplier<Either<SerializedValue<TaskInformation>, PermanentBlobKey>> taskInfoSupplier,
		JobID jobID,
		boolean lazyScheduling,
		int subtaskIndex,
		ExecutionEdge[][] inputEdges) {

		this.executionId = executionId;
		this.attemptNumber = attemptNumber;
		this.serializedJobInformation = serializedJobInformation;
		this.taskInfoSupplier = taskInfoSupplier;
		this.jobID = jobID;
		this.lazyScheduling = lazyScheduling;
		this.subtaskIndex = subtaskIndex;
		this.inputEdges = inputEdges;
	}

	public static TaskDeploymentDescriptorFactory fromExecutionVertex(
		ExecutionVertex executionVertex, ExecutionAttemptID executionId, int attemptNumber) {

		ExecutionGraph executionGraph = executionVertex.getExecutionGraph();

		return new TaskDeploymentDescriptorFactory(
			executionId,
			attemptNumber,
			getSerializedJobInformation(executionGraph),
			() -> executionVertex.getJobVertex().getTaskInformationOrBlobKey(),
			executionGraph.getJobID(),
			executionGraph.getScheduleMode().allowLazyDeployment(),
			executionVertex.getParallelSubtaskIndex(),
			executionVertex.getAllInputEdges());
	}

	private static TaskDeploymentDescriptor.MaybeOffloaded<JobInformation> getSerializedJobInformation(
		ExecutionGraph executionGraph) {

		Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey =
			executionGraph.getJobInformationOrBlobKey();

		if (jobInformationOrBlobKey.isLeft()) {
			return new TaskDeploymentDescriptor.NonOffloaded<>(jobInformationOrBlobKey.left());
		} else {
			return new TaskDeploymentDescriptor.Offloaded<>(jobInformationOrBlobKey.right());
		}
	}

	/**
	 * Creates a task deployment descriptor to deploy a subtask to the given target slot.
	 */
	public TaskDeploymentDescriptor createDeploymentDescriptor(
		LogicalSlot targetSlot,
		@Nullable JobManagerTaskRestore taskRestore,
		Collection<ResultPartitionDeploymentDescriptor> producedPartitions) throws Exception {

		return createDeploymentDescriptor(
			targetSlot.getTaskManagerLocation().getResourceID(),
			targetSlot.getAllocationId(),
			targetSlot.getPhysicalSlotNumber(),
			taskRestore,
			producedPartitions);
	}

	/**
	 * Creates a task deployment descriptor to deploy a subtask.
	 */
	@VisibleForTesting
	public TaskDeploymentDescriptor createDeploymentDescriptor(
		ResourceID consumerResourceId,
		AllocationID allocationID,
		int targetSlotNumber,
		@Nullable JobManagerTaskRestore taskRestore,
		Collection<ResultPartitionDeploymentDescriptor> producedPartitions) throws Exception {

		return new TaskDeploymentDescriptor(
			jobID,
			serializedJobInformation,
			getSerializedTaskInformation(),
			executionId,
			allocationID,
			subtaskIndex,
			attemptNumber,
			targetSlotNumber,
			taskRestore,
			new ArrayList<>(producedPartitions),
			createConsumedGates(consumerResourceId, inputEdges, subtaskIndex, lazyScheduling));
	}

	private TaskDeploymentDescriptor.MaybeOffloaded<TaskInformation> getSerializedTaskInformation() throws Exception {
		Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey = taskInfoSupplier.get();

		if (taskInformationOrBlobKey.isLeft()) {
			return new TaskDeploymentDescriptor.NonOffloaded<>(taskInformationOrBlobKey.left());
		} else {
			return new TaskDeploymentDescriptor.Offloaded<>(taskInformationOrBlobKey.right());
		}
	}

	private static List<InputGateDeploymentDescriptor> createConsumedGates(
		ResourceID consumerResourceId,
		ExecutionEdge[][] inputEdges,
		int subtaskIndex,
		boolean allowLazyDeployment) throws ExecutionGraphException {

		// Consumed intermediate results
		List<InputGateDeploymentDescriptor> consumedPartitions = new ArrayList<>(inputEdges.length);

		for (ExecutionEdge[] edges : inputEdges) {
			ShuffleDeploymentDescriptor[] sdd = getConsumedPartitionSdds(edges, allowLazyDeployment);
			// If the produced partition has multiple consumers registered, we
			// need to request the one matching our sub task index.
			// TODO Refactor after removing the consumers from the intermediate result partitions
			int numConsumerEdges = edges[0].getSource().getConsumers().get(0).size();

			int queueToRequest = subtaskIndex % numConsumerEdges;

			IntermediateResult consumedIntermediateResult = edges[0].getSource().getIntermediateResult();
			final IntermediateDataSetID resultId = consumedIntermediateResult.getId();
			final ResultPartitionType partitionType = consumedIntermediateResult.getResultType();

			consumedPartitions.add(new InputGateDeploymentDescriptor(
				resultId, partitionType, queueToRequest, sdd, consumerResourceId));
		}

		return consumedPartitions;
	}

	private static ShuffleDeploymentDescriptor[] getConsumedPartitionSdds(
		ExecutionEdge[] edges, boolean allowLazyDeployment) throws ExecutionGraphException {

		final ShuffleDeploymentDescriptor[] sdd = new ShuffleDeploymentDescriptor[edges.length];
		// Each edge is connected to a different result partition
		for (int i = 0; i < edges.length; i++) {
			sdd[i] = getConsumedPartitionSdd(edges[i], allowLazyDeployment);
		}
		return sdd;
	}

	private static ShuffleDeploymentDescriptor getConsumedPartitionSdd(
		ExecutionEdge edge,
		boolean allowLazyDeployment) throws ExecutionGraphException {

		final IntermediateResultPartition consumedPartition = edge.getSource();
		final Execution producer = consumedPartition.getProducer().getCurrentExecutionAttempt();

		final ExecutionState producerState = producer.getState();
		final Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions =
			producer.getProducedPartitions();

		final ResultPartitionID consumedPartitionId = new ResultPartitionID(
			consumedPartition.getPartitionId(), producer.getAttemptId());

		return getConsumedPartitionSdd(consumedPartitionId, consumedPartition.getResultType(),
			consumedPartition.isConsumable(), producerState, allowLazyDeployment, producedPartitions);
	}

	@VisibleForTesting
	static ShuffleDeploymentDescriptor getConsumedPartitionSdd(
		ResultPartitionID consumedPartitionId,
		ResultPartitionType resultPartitionType,
		boolean isConsumable,
		ExecutionState producerState,
		boolean allowLazyDeployment,
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions)
		throws ExecutionGraphException {

		// The producing task needs to be RUNNING or already FINISHED
		if ((resultPartitionType.isPipelined() || isConsumable) &&
			checkInputReady(consumedPartitionId.getPartitionId(), producedPartitions) &&
			isProducerAvailable(producerState)) {
			// partition is already registered
			return producedPartitions.get(consumedPartitionId.getPartitionId()).getShuffleDeploymentDescriptor();
		}
		else if (allowLazyDeployment) {
			// The producing task might not have registered the partition yet
			return new UnknownShuffleDeploymentDescriptor(consumedPartitionId);
		}
		else {
			return handleConsumedPartitionSddErrors(
				consumedPartitionId, resultPartitionType, isConsumable, producerState);
		}
	}

	private static ShuffleDeploymentDescriptor handleConsumedPartitionSddErrors(
		ResultPartitionID consumedPartitionId,
		ResultPartitionType resultPartitionType,
		boolean isConsumable,
		ExecutionState producerState) throws ExecutionGraphException {

		if (isProducerFailedOrCanceled(producerState)) {
			String msg = "Trying to schedule a task whose inputs were canceled or failed. " +
				"The producer is in state " + producerState + ".";
			throw new ExecutionGraphException(msg);
		}
		else {
			String msg = String.format("Trying to eagerly schedule a task whose inputs " +
					"are not ready (result type: %s, partition consumable: %s, producer state: %s, partition id: %s).",
				resultPartitionType,
				isConsumable,
				producerState,
				consumedPartitionId);
			throw new ExecutionGraphException(msg);
		}
	}

	public static ShuffleDeploymentDescriptor getKnownConsumedPartitionSdd(ExecutionEdge edge) {
		IntermediateResultPartition consumedPartition = edge.getSource();
		Execution producer = consumedPartition.getProducer().getCurrentExecutionAttempt();
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions =
			producer.getProducedPartitions();
		Preconditions.checkArgument(checkInputReady(consumedPartition.getPartitionId(), producedPartitions),
			"Known partitions are expected to be already cached during producer deployment");
		return producedPartitions.get(consumedPartition.getPartitionId()).getShuffleDeploymentDescriptor();
	}

	private static boolean checkInputReady(
		IntermediateResultPartitionID id,
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions) {

		return producedPartitions != null && producedPartitions.containsKey(id);
	}

	private static boolean isProducerAvailable(ExecutionState producerState) {
		return producerState == ExecutionState.RUNNING ||
			producerState == ExecutionState.FINISHED ||
			producerState == ExecutionState.SCHEDULED ||
			producerState == ExecutionState.DEPLOYING;
	}

	private static boolean isProducerFailedOrCanceled(ExecutionState producerState) {
		return producerState == ExecutionState.CANCELING
			|| producerState == ExecutionState.CANCELED
			|| producerState == ExecutionState.FAILED;
	}
}
