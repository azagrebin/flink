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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nonnull;

import java.net.InetSocketAddress;

/**
 * Partition producer descriptor for {@link ShuffleMaster} to obtain {@link ShuffleDeploymentDescriptor}.
 */
public class ProducerShuffleDescriptor {
	/** The resource ID to identify the container where the producer execution is deployed. */
	@Nonnull
	private final ResourceID producerResourceId;

	/** The address to use to request the remote partition. */
	@Nonnull
	private final InetSocketAddress address;

	/** The ID of the producer execution attempt. */
	@Nonnull
	private final ExecutionAttemptID producerExecutionId;

	public ProducerShuffleDescriptor(
		@Nonnull ResourceID producerResourceId,
		@Nonnull InetSocketAddress address,
		@Nonnull ExecutionAttemptID producerExecutionId) {

		this.producerResourceId = producerResourceId;
		this.address = address;
		this.producerExecutionId = producerExecutionId;
	}

	@Nonnull
	public ResourceID getProducerResourceId() {
		return producerResourceId;
	}

	@Nonnull
	public InetSocketAddress getAddress() {
		return address;
	}

	@Nonnull
	public ExecutionAttemptID getProducerExecutionId() {
		return producerExecutionId;
	}

	public static ProducerShuffleDescriptor create(TaskManagerLocation connectionInfo, ExecutionAttemptID attemptId) {
		int port = connectionInfo.dataPort() > 0 ? connectionInfo.dataPort() : 0;
		InetSocketAddress address = new InetSocketAddress(connectionInfo.address(), port);
		return new ProducerShuffleDescriptor(connectionInfo.getResourceID(), address, attemptId);
	}
}
