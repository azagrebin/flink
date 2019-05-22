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
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
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
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Factory of {@link TaskDeploymentDescriptor} to deploy {@link org.apache.flink.runtime.taskmanager.Task} from {@link Execution}.
 */
public class TaskDeploymentDescriptorFactory {
	private final ExecutionAttemptID executionId;
	private final int attemptNumber;
	private final MaybeOffloaded<JobInformation> serializedJobInformation;
	private final MaybeOffloaded<TaskInformation> taskInfo;
	private final JobID jobID;
	private final boolean lazyScheduling;
	private final int subtaskIndex;
	private final ExecutionEdge[][] inputEdges;

	private TaskDeploymentDescriptorFactory(
			ExecutionAttemptID executionId,
			int attemptNumber,
			MaybeOffloaded<JobInformation> serializedJobInformation,
			MaybeOffloaded<TaskInformation> taskInfo,
			JobID jobID,
			boolean lazyScheduling,
			int subtaskIndex,
			ExecutionEdge[][] inputEdges) {
		this.executionId = executionId;
		this.attemptNumber = attemptNumber;
		this.serializedJobInformation = serializedJobInformation;
		this.taskInfo = taskInfo;
		this.jobID = jobID;
		this.lazyScheduling = lazyScheduling;
		this.subtaskIndex = subtaskIndex;
		this.inputEdges = inputEdges;
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
			taskInfo,
			executionId,
			allocationID,
			subtaskIndex,
			attemptNumber,
			targetSlotNumber,
			taskRestore,
			new ArrayList<>(producedPartitions),
			createInputGateDeploymentDescriptors(consumerResourceId, inputEdges, subtaskIndex, lazyScheduling));
	}

	public static TaskDeploymentDescriptorFactory fromExecutionVertex(
		ExecutionVertex executionVertex,
		ExecutionAttemptID executionId,
		int attemptNumber) throws ExecutionGraphException {
		ExecutionGraph executionGraph = executionVertex.getExecutionGraph();
		return new TaskDeploymentDescriptorFactory(
			executionId,
			attemptNumber,
			getSerializedJobInformation(executionGraph),
			getSerializedTaskInformation(executionVertex.getJobVertex().getTaskInformationOrBlobKey()),
			executionGraph.getJobID(),
			executionGraph.getScheduleMode().allowLazyDeployment(),
			executionVertex.getParallelSubtaskIndex(),
			executionVertex.getAllInputEdges());
	}

	private static MaybeOffloaded<JobInformation> getSerializedJobInformation(
		ExecutionGraph executionGraph) {
		Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey =
			executionGraph.getJobInformationOrBlobKey();
		if (jobInformationOrBlobKey.isLeft()) {
			return new TaskDeploymentDescriptor.NonOffloaded<>(jobInformationOrBlobKey.left());
		} else {
			return new TaskDeploymentDescriptor.Offloaded<>(jobInformationOrBlobKey.right());
		}
	}

	private static MaybeOffloaded<TaskInformation> getSerializedTaskInformation(
		Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInfo) {
		return taskInfo.isLeft() ?
			new TaskDeploymentDescriptor.NonOffloaded<>(taskInfo.left()) :
			new TaskDeploymentDescriptor.Offloaded<>(taskInfo.right());
	}

	private static List<InputGateDeploymentDescriptor> createInputGateDeploymentDescriptors(
			ResourceID consumerResourceId,
			ExecutionEdge[][] inputEdges,
			int subtaskIndex,
			boolean allowLazyDeployment) throws ExecutionGraphException {
		List<InputGateDeploymentDescriptor> consumedPartitions = new ArrayList<>(inputEdges.length);

		for (ExecutionEdge[] edges : inputEdges) {
			ShuffleDescriptor[] consumedPartitionShuffleDescriptors =
				getConsumedPartitionShuffleDeploymentDescriptors(edges, allowLazyDeployment);
			// If the produced partition has multiple consumers registered, we
			// need to request the one matching our sub task index.
			// TODO Refactor after removing the consumers from the intermediate result partitions
			int numConsumerEdges = edges[0].getSource().getConsumers().get(0).size();

			int queueToRequest = subtaskIndex % numConsumerEdges;

			IntermediateResult consumedIntermediateResult = edges[0].getSource().getIntermediateResult();
			IntermediateDataSetID resultId = consumedIntermediateResult.getId();
			ResultPartitionType partitionType = consumedIntermediateResult.getResultType();

			consumedPartitions.add(new InputGateDeploymentDescriptor(
				resultId,
				partitionType,
				queueToRequest,
				consumedPartitionShuffleDescriptors,
				consumerResourceId));
		}

		return consumedPartitions;
	}

	private static ShuffleDescriptor[] getConsumedPartitionShuffleDeploymentDescriptors(
			ExecutionEdge[] edges, boolean allowLazyDeployment) throws ExecutionGraphException {
		ShuffleDescriptor[] shuffleDescriptors = new ShuffleDescriptor[edges.length];
		// Each edge is connected to a different result partition
		for (int i = 0; i < edges.length; i++) {
			shuffleDescriptors[i] =
				getConsumedPartitionShuffleDeploymentDescriptor(edges[i], allowLazyDeployment);
		}
		return shuffleDescriptors;
	}

	private static ShuffleDescriptor getConsumedPartitionShuffleDeploymentDescriptor(
			ExecutionEdge edge,
			boolean allowLazyDeployment) throws ExecutionGraphException {
		IntermediateResultPartition consumedPartition = edge.getSource();
		Execution producer = consumedPartition.getProducer().getCurrentExecutionAttempt();

		ExecutionState producerState = producer.getState();
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions =
			producer.getProducedPartitions();

		ResultPartitionID consumedPartitionId = new ResultPartitionID(
			consumedPartition.getPartitionId(),
			producer.getAttemptId());

		return getConsumedPartitionShuffleDeploymentDescriptor(
			consumedPartitionId,
			consumedPartition.getResultType(),
			consumedPartition.isConsumable(),
			producerState,
			allowLazyDeployment,
			producedPartitions);
	}

	@VisibleForTesting
	static ShuffleDescriptor getConsumedPartitionShuffleDeploymentDescriptor(
			ResultPartitionID consumedPartitionId,
			ResultPartitionType resultPartitionType,
			boolean isConsumable,
			ExecutionState producerState,
			boolean allowLazyDeployment,
			Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions) throws ExecutionGraphException {
		// The producing task needs to be RUNNING or already FINISHED
		if ((resultPartitionType.isPipelined() || isConsumable) &&
			checkInputReady(consumedPartitionId.getPartitionId(), producedPartitions) &&
			isProducerAvailable(producerState)) {
			// partition is already registered
			return producedPartitions.get(consumedPartitionId.getPartitionId()).getShuffleDescriptor();
		}
		else if (allowLazyDeployment) {
			// The producing task might not have registered the partition yet
			return new UnknownShuffleDescriptor(consumedPartitionId);
		}
		else {
			// throw respective exceptions
			handleConsumedPartitionShuffleDeploymentDescriptorErrors(
				consumedPartitionId,
				resultPartitionType,
				isConsumable,
				producerState);
			return null;
		}
	}

	private static void handleConsumedPartitionShuffleDeploymentDescriptorErrors(
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

	public static ShuffleDescriptor getKnownConsumedPartitionShuffleDeploymentDescriptor(ExecutionEdge edge) {
		IntermediateResultPartition consumedPartition = edge.getSource();
		Execution producer = consumedPartition.getProducer().getCurrentExecutionAttempt();
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions =
			producer.getProducedPartitions();
		checkState(checkInputReady(consumedPartition.getPartitionId(), producedPartitions),
			"Known partitions are expected to be already cached during producer deployment");
		return producedPartitions.get(consumedPartition.getPartitionId()).getShuffleDescriptor();
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
		return producerState == ExecutionState.CANCELING ||
			producerState == ExecutionState.CANCELED ||
			producerState == ExecutionState.FAILED;
	}
}
