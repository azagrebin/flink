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

import java.net.InetSocketAddress;

/**
 * Partition producer descriptor for {@link ShuffleMaster} to obtain {@link ShuffleDescriptor}.
 */
public class ProducerDescriptor {
	/** The resource ID to identify the container where the producer execution is deployed. */
	private final ResourceID producerResourceId;

	/** The address to connect to the producer. */
	private final InetSocketAddress address;

	/** The ID of the producer execution attempt. */
	private final ExecutionAttemptID producerExecutionId;

	public ProducerDescriptor(
			ResourceID producerResourceId,
			InetSocketAddress address,
			ExecutionAttemptID producerExecutionId) {
		this.producerResourceId = producerResourceId;
		this.address = address;
		this.producerExecutionId = producerExecutionId;
	}

	ResourceID getProducerResourceId() {
		return producerResourceId;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	ExecutionAttemptID getProducerExecutionId() {
		return producerExecutionId;
	}

	public static ProducerDescriptor create(TaskManagerLocation producerLocation, ExecutionAttemptID attemptId) {
		int port = producerLocation.dataPort() > 0 ? producerLocation.dataPort() : 0;
		InetSocketAddress address = new InetSocketAddress(producerLocation.address(), port);
		return new ProducerDescriptor(producerLocation.getResourceID(), address, attemptId);
	}
}
