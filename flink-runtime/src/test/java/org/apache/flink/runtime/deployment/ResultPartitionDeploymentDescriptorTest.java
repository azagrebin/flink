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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link ResultPartitionDeploymentDescriptor}.
 */
public class ResultPartitionDeploymentDescriptorTest {

	/**
	 * Tests simple de/serialization.
	 */
	@Test
	public void testSerialization() throws Exception {
		// Expected values
		IntermediateDataSetID resultId = new IntermediateDataSetID();

		IntermediateResultPartitionID partitionId = new IntermediateResultPartitionID();
		ExecutionAttemptID producerExecutionId = new ExecutionAttemptID();

		ResultPartitionType partitionType = ResultPartitionType.PIPELINED;
		int numberOfSubpartitions = 24;
		int connectionIndex = 10;

		PartitionDescriptor partitionDescriptor = new PartitionDescriptor(
			resultId,
			partitionId,
			partitionType,
			numberOfSubpartitions,
			connectionIndex);

		ResourceID producerLocation = new ResourceID("producerLocation");
		InetSocketAddress address = new InetSocketAddress("localhost", 10000);
		ResultPartitionID resultPartitionID = new ResultPartitionID(partitionId, producerExecutionId);
		ConnectionID connectionID = new ConnectionID(address, connectionIndex);
		ShuffleDescriptor shuffleDescriptor = new NettyShuffleDescriptor(
			producerLocation,
			connectionID,
			resultPartitionID);

		ResultPartitionDeploymentDescriptor orig = new ResultPartitionDeploymentDescriptor(
			partitionDescriptor,
			shuffleDescriptor,
			numberOfSubpartitions,
			true);

		ResultPartitionDeploymentDescriptor copy = CommonTestUtils.createCopySerializable(orig);

		assertTrue(copy.getShuffleDescriptor() instanceof NettyShuffleDescriptor);
		NettyShuffleDescriptor copySdd = (NettyShuffleDescriptor) copy.getShuffleDescriptor();
		assertTrue(copySdd.isLocalTo(producerLocation));
		assertEquals(address, copySdd.getConnectionId().getAddress());
		assertEquals(connectionIndex, copySdd.getConnectionId().getConnectionIndex());

		assertEquals(resultId, copy.getResultId());
		assertEquals(partitionId, copy.getPartitionId());
		assertEquals(partitionType, copy.getPartitionType());
		assertEquals(numberOfSubpartitions, copy.getNumberOfSubpartitions());
		assertTrue(copy.sendScheduleOrUpdateConsumersMessage());
	}
}
