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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDeploymentDescriptor;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Contains information where to find a partition. The partition is defined by the
 * {@link IntermediateDataSetID} and the partition is specified by
 * {@link org.apache.flink.runtime.shuffle.ShuffleDeploymentDescriptor}.
 */
public class PartitionInfo implements Serializable {

	private static final long serialVersionUID = 1724490660830968430L;

	@Nonnull
	private final IntermediateDataSetID intermediateDataSetID;

	@Nonnull
	private final ResourceID consumerResourceID;

	@Nonnull
	private final ShuffleDeploymentDescriptor shuffleDeploymentDescriptor;

	public PartitionInfo(
		@Nonnull IntermediateDataSetID intermediateResultPartitionID,
		@Nonnull ResourceID consumerResourceID,
		@Nonnull ShuffleDeploymentDescriptor shuffleDeploymentDescriptor) {

		this.intermediateDataSetID = intermediateResultPartitionID;
		this.consumerResourceID = consumerResourceID;
		this.shuffleDeploymentDescriptor = shuffleDeploymentDescriptor;
	}

	@Nonnull
	public IntermediateDataSetID getIntermediateDataSetID() {
		return intermediateDataSetID;
	}

	@Nonnull
	public ResourceID getConsumerResourceID() {
		return consumerResourceID;
	}

	@Nonnull
	public ShuffleDeploymentDescriptor getShuffleDeploymentDescriptor() {
		return shuffleDeploymentDescriptor;
	}

	// ------------------------------------------------------------------------

	static PartitionInfo fromEdge(ExecutionEdge executionEdge) {
		IntermediateDataSetID intermediateDataSetID = executionEdge.getSource().getIntermediateResult().getId();
		ResourceID consumerResourceID = executionEdge.getTarget().getCurrentExecutionAttempt()
			.getAssignedResource().getTaskManagerLocation().getResourceID();
		ShuffleDeploymentDescriptor sdd = TaskDeploymentDescriptorFactory.getKnownConsumedPartitionSdd(executionEdge);
		return new PartitionInfo(intermediateDataSetID, consumerResourceID, sdd);
	}
}
