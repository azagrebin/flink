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

import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.PartitionShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDeploymentDescriptor;

import javax.annotation.Nonnull;

import java.io.Serializable;

/**
 * Deployment descriptor for a result partition.
 *
 * @see ResultPartition
 */
public class ResultPartitionDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = 6343547936086963705L;

	@Nonnull
	private final PartitionShuffleDescriptor partitionShuffleDescriptor;

	@Nonnull
	private final ShuffleDeploymentDescriptor shuffleDeploymentDescriptor;

	/** Flag whether the result partition should send scheduleOrUpdateConsumer messages. */
	private final boolean sendScheduleOrUpdateConsumersMessage;

	public ResultPartitionDeploymentDescriptor(
		@Nonnull PartitionShuffleDescriptor partitionShuffleDescriptor,
		@Nonnull ShuffleDeploymentDescriptor shuffleDeploymentDescriptor,
		boolean sendScheduleOrUpdateConsumersMessage) {

		this.partitionShuffleDescriptor = partitionShuffleDescriptor;
		this.shuffleDeploymentDescriptor = shuffleDeploymentDescriptor;
		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;
	}

	@Nonnull
	public IntermediateDataSetID getResultId() {
		return partitionShuffleDescriptor.getResultId();
	}

	@Nonnull
	public IntermediateResultPartitionID getPartitionId() {
		return partitionShuffleDescriptor.getPartitionId();
	}

	@Nonnull
	public ResultPartitionType getPartitionType() {
		return partitionShuffleDescriptor.getPartitionType();
	}

	public int getNumberOfSubpartitions() {
		return partitionShuffleDescriptor.getNumberOfSubpartitions();
	}

	public int getMaxParallelism() {
		return partitionShuffleDescriptor.getMaxParallelism();
	}

	@Nonnull
	ShuffleDeploymentDescriptor getShuffleDeploymentDescriptor() {
		return shuffleDeploymentDescriptor;
	}

	public boolean sendScheduleOrUpdateConsumersMessage() {
		return sendScheduleOrUpdateConsumersMessage;
	}

	@Override
	public String toString() {
		return String.format("ResultPartitionDeploymentDescriptor [PartitionShuffleDescriptor: %s, "
						+ "ShuffleDeploymentDescriptor: %s]",
			partitionShuffleDescriptor, shuffleDeploymentDescriptor);
	}
}
