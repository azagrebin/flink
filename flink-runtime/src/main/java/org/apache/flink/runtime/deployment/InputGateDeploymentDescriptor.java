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
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import javax.annotation.Nonnegative;

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
	private final IntermediateDataSetID consumedResultId;

	/** The type of the partition the input gate is going to consume. */
	private final ResultPartitionType consumedPartitionType;

	/**
	 * The index of the consumed subpartition of each consumed partition. This index depends on the
	 * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
	 */
	@Nonnegative
	private final int consumedSubpartitionIndex;

	/** An input channel for each consumed subpartition. */
	private final ShuffleDescriptor[] inputChannels;

	/**
	 * {@link ResourceID} of partition consume to identify its location.
	 *
	 * <p>It can be used e.g. to compare with partition producer {@link ResourceID} in
	 * {@link ProducerDescriptor} to determine producer/consumer co-location.
	 */
	private final ResourceID consumerLocation;

	public InputGateDeploymentDescriptor(
			IntermediateDataSetID consumedResultId,
			ResultPartitionType consumedPartitionType,
			@Nonnegative int consumedSubpartitionIndex,
			ShuffleDescriptor[] inputChannels,
			ResourceID consumerLocation) {
		this.consumedResultId = consumedResultId;
		this.consumedPartitionType = consumedPartitionType;
		this.consumerLocation = consumerLocation;
		this.consumedSubpartitionIndex = consumedSubpartitionIndex;
		this.inputChannels = inputChannels;
	}

	public IntermediateDataSetID getConsumedResultId() {
		return consumedResultId;
	}

	/**
	 * Returns the type of this input channel's consumed result partition.
	 *
	 * @return consumed result partition type
	 */
	public ResultPartitionType getConsumedPartitionType() {
		return consumedPartitionType;
	}

	@Nonnegative
	public int getConsumedSubpartitionIndex() {
		return consumedSubpartitionIndex;
	}

	public ShuffleDescriptor[] getInputChannelDescriptors() {
		return inputChannels;
	}

	public ResourceID getConsumerLocation() {
		return consumerLocation;
	}

	@Override
	public String toString() {
		return String.format("InputGateDeploymentDescriptor [result id: %s, " +
						"consumed subpartition index: %d, input channels: %s]",
				consumedResultId.toString(), consumedSubpartitionIndex,
				Arrays.toString(inputChannels));
	}
}
