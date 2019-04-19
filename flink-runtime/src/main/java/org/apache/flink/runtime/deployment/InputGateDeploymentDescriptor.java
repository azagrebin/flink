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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDeploymentDescriptor;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Deployment descriptor for a single input gate instance.
 *
 * <p>Each input gate consumes partitions of a single intermediate result. The consumed
 * subpartition index is the same for each consumed partition.
 *
 * @see SingleInputGate
 */
public class InputGateDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = -7143441863165366704L;
	/**
	 * The ID of the consumed intermediate result. Each input gate consumes partitions of the
	 * intermediate result specified by this ID. This ID also identifies the input gate at the
	 * consuming task.
	 */
	@Nonnull
	private final IntermediateDataSetID consumedResultId;

	/** The type of the partition the input gate is going to consume. */
	@Nonnull
	private final ResultPartitionType consumedPartitionType;

	/**
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
	 */
	private final int consumedSubpartitionIndex;

	/** An input channel for each consumed subpartition. */
	@Nonnull
	private final ShuffleDeploymentDescriptor[] inputChannels;

	/**
	 * {@link ResourceID} of partition consumer.
	 *
	 * <p>It can be used e.g. to compare with partition producer {@link ResourceID} in
	 * {@link org.apache.flink.runtime.shuffle.ProducerShuffleDescriptor} to determine producer/consumer co-location.
	 */
	@Nonnull
	private final ResourceID consumerResourceId;

	public InputGateDeploymentDescriptor(
		@Nonnull IntermediateDataSetID consumedResultId,
		@Nonnull ResultPartitionType consumedPartitionType,
		@Nonnegative int consumedSubpartitionIndex,
		@Nonnull ShuffleDeploymentDescriptor[] inputChannels,
		@Nonnull ResourceID consumerResourceId) {

		this.consumedResultId = consumedResultId;
		this.consumedPartitionType = consumedPartitionType;
		this.consumerResourceId = consumerResourceId;
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;
		this.inputChannels = inputChannels;
	}

	@Nonnull
	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	/**
	 * Returns the type of this input channel's consumed result partition.
	 *
	 * @return consumed result partition type
	 */
	@Nonnull
	public ResultPartitionType getConsumedPartitionType() {
		return consumedPartitionType;
	}

	@Nonnegative
	public int getConsumedSubpartitionIndex() {
		return consumedSubpartitionIndex;
	}

	@Nonnull
	public ShuffleDeploymentDescriptor[] getShuffleDeploymentDescriptors() {
		return inputChannels;
	}

	@Nonnull
	public ResourceID getConsumerResourceId() {
		return consumerResourceId;
	}

	@Override
	public String toString() {
		return String.format("InputGateDeploymentDescriptor [result id: %s, " +
						"consumed subpartition index: %d, input channels: %s]",
				consumedResultId.toString(), consumedSubpartitionIndex,
				Arrays.toString(inputChannels));
	}
}
