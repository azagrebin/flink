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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.DefaultShuffleDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDeploymentDescriptor;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel.isChannelLocal;

/**
 * Factory for {@link SingleInputGate} to use in {@link org.apache.flink.runtime.io.network.NetworkEnvironment}.
 */
public class SingleInputGateFactory {
	private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

	private final boolean isCreditBased;

	private final int partitionRequestInitialBackoff;

	private final int partitionRequestMaxBackoff;

	@Nonnull
	private final ConnectionManager connectionManager;

	@Nonnull
	private final ResultPartitionManager partitionManager;

	@Nonnull
	private final TaskEventPublisher taskEventPublisher;

	@Nonnull
	private final NetworkBufferPool networkBufferPool;

	private final int networkBuffersPerChannel;

	private final int floatingNetworkBuffersPerGate;

	public SingleInputGateFactory(
			@Nonnull NetworkEnvironmentConfiguration networkConfig,
			@Nonnull ConnectionManager connectionManager,
			@Nonnull ResultPartitionManager partitionManager,
			@Nonnull TaskEventPublisher taskEventPublisher,
			@Nonnull NetworkBufferPool networkBufferPool) {
		this.isCreditBased = networkConfig.isCreditBased();
		this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
		this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
		this.networkBuffersPerChannel = networkConfig.networkBuffersPerChannel();
		this.floatingNetworkBuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
		this.connectionManager = connectionManager;
		this.partitionManager = partitionManager;
		this.taskEventPublisher = taskEventPublisher;
		this.networkBufferPool = networkBufferPool;
	}

	/**
	 * Creates an input gate and all of its input channels.
	 */
	public SingleInputGate create(
			@Nonnull String owningTaskName,
			@Nonnull JobID jobId,
			@Nonnull InputGateDeploymentDescriptor igdd,
			@Nonnull TaskActions taskActions,
			@Nonnull InputChannelMetrics metrics,
			@Nonnull Counter numBytesInCounter) {
		SingleInputGate inputGate = new SingleInputGate(
			owningTaskName,
			jobId,
			igdd.getConsumedResultId(),
			igdd.getConsumedPartitionType(),
			igdd.getConsumedSubpartitionIndex(),
			igdd.getShuffleDeploymentDescriptors().length,
			taskActions,
			numBytesInCounter,
			isCreditBased,
			createBufferPoolFactory(igdd.getShuffleDeploymentDescriptors().length, igdd.getConsumedPartitionType()));
		createInputChannels(owningTaskName, igdd, inputGate, metrics);
		return inputGate;
	}

	private void createInputChannels(
			String owningTaskName,
			InputGateDeploymentDescriptor igdd,
			SingleInputGate inputGate,
			InputChannelMetrics metrics) {
		ShuffleDeploymentDescriptor[] sdd = igdd.getShuffleDeploymentDescriptors();

		// Create the input channels. There is one input channel for each consumed partition.
		final InputChannel[] inputChannels = new InputChannel[sdd.length];

		ChannelStat channelStat = new ChannelStat();

		for (int i = 0; i < inputChannels.length; i++) {
			boolean isLocal = isChannelLocal(sdd[i], igdd.getConsumerResourceId());
			inputChannels[i] = createInputChannel(inputGate, i, isLocal, sdd[i], channelStat, metrics);
			inputGate.setInputChannel(inputChannels[i].getPartitionId().getPartitionId(), inputChannels[i]);
		}

		LOG.debug("{}: Created {} input channels (local: {}, remote: {}, unknown: {}).",
			owningTaskName,
			inputChannels.length,
			channelStat.numLocalChannels,
			channelStat.numRemoteChannels,
			channelStat.numUnknownChannels);
	}

	private InputChannel createInputChannel(
			SingleInputGate inputGate,
			int index,
			boolean isLocal,
			ShuffleDeploymentDescriptor sdd,
			ChannelStat channelStat,
			InputChannelMetrics metrics) {
		if (sdd instanceof UnknownShuffleDeploymentDescriptor) {
			ResultPartitionID partitionId = ((UnknownShuffleDeploymentDescriptor) sdd).getResultPartitionID();
			channelStat.numUnknownChannels++;
			return new UnknownInputChannel(
				inputGate,
				index,
				partitionId,
				partitionManager,
				taskEventPublisher,
				connectionManager,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				metrics,
				networkBufferPool);
		} else if (sdd instanceof DefaultShuffleDeploymentDescriptor) {
			return createKnownInputChannel(inputGate, index, isLocal,
				(DefaultShuffleDeploymentDescriptor) sdd, channelStat, taskEventPublisher, metrics);
		} else {
			throw new IllegalArgumentException(String.format(
				"Default network shuffle service: unsupported ShuffleDeploymentDescriptor <%s>",
				sdd.getClass().getName()));
		}
	}

	private InputChannel createKnownInputChannel(
			SingleInputGate inputGate,
			int index,
			boolean isLocal,
			DefaultShuffleDeploymentDescriptor defaultSdd,
			ChannelStat channelStat,
			TaskEventPublisher taskEventPublisher,
			InputChannelMetrics metrics) {
		ResultPartitionID partitionId = defaultSdd.getResultPartitionID();
		if (isLocal) {
			// Consuming task is deployed to the same TaskManager as the partition => local
			channelStat.numLocalChannels++;
			return new LocalInputChannel(inputGate, index, partitionId,
				partitionManager,
				taskEventPublisher,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				metrics);
		} else {
			// Different instances => remote
			channelStat.numRemoteChannels++;
			return new RemoteInputChannel(inputGate, index, partitionId,
				defaultSdd.getConnectionId(),
				connectionManager,
				partitionRequestInitialBackoff,
				partitionRequestMaxBackoff,
				metrics,
				networkBufferPool);
		}
	}

	private SupplierWithException<BufferPool, IOException> createBufferPoolFactory(int size, ResultPartitionType type) {
		return createBufferPoolFactory(
			networkBufferPool,
			isCreditBased,
			networkBuffersPerChannel,
			floatingNetworkBuffersPerGate,
			size,
			type);
	}

	@VisibleForTesting
	static SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
			BufferPoolFactory bufferPoolFactory,
			boolean isCreditBased,
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate,
			int size,
			ResultPartitionType type) {
		if (isCreditBased) {
			int maxNumberOfMemorySegments = type.isBounded() ? floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
			return () -> bufferPoolFactory.createBufferPool(0, maxNumberOfMemorySegments);
		} else {
			int maxNumberOfMemorySegments = type.isBounded() ?
				size * networkBuffersPerChannel +
					floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
			return () -> bufferPoolFactory.createBufferPool(size, maxNumberOfMemorySegments);
		}
	}

	private static class ChannelStat {
		int numLocalChannels = 0;
		int numRemoteChannels = 0;
		int numUnknownChannels = 0;
	}
}
