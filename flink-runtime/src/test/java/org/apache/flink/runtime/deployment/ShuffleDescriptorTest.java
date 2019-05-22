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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraphException;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ShuffleDescriptor}.
 */
public class ShuffleDescriptorTest extends TestLogger {
	private static final ConnectionID CONNECTION =
		new ConnectionID(new InetSocketAddress(InetAddress.getLoopbackAddress(), 5000), 0);

	/**
	 * Tests the deployment descriptors for local, remote, and unknown partition
	 * locations (with lazy deployment allowed and all execution states for the
	 * producers).
	 */
	@Test
	public void testMixedLocalRemoteUnknownDeployment() throws Exception {
		ResourceID consumerResourceID = ResourceID.generate();

		// Local and remote channel are only allowed for certain execution
		// states.
		for (ExecutionState state : ExecutionState.values()) {
			Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions = new HashMap<>();

			ResultPartitionID localPartitionId = new ResultPartitionID();
			producedPartitions.put(localPartitionId.getPartitionId(),
				createResultPartitionDeploymentDescriptor(localPartitionId, consumerResourceID));

			ResultPartitionID remotePartitionId = new ResultPartitionID();
			producedPartitions.put(remotePartitionId.getPartitionId(),
				createResultPartitionDeploymentDescriptor(remotePartitionId, ResourceID.generate()));

			ResultPartitionID unknownPartitionId = new ResultPartitionID();

			ShuffleDescriptor localSdd =
				getConsumedPartitionSdd(localPartitionId, state, producedPartitions, true);
			ShuffleDescriptor remoteSdd =
				getConsumedPartitionSdd(remotePartitionId, state, producedPartitions, true);
			ShuffleDescriptor unknownSdd =
				getConsumedPartitionSdd(unknownPartitionId, state, producedPartitions, true);

			UnknownShuffleDescriptor typedUnknownSdd;

			// These states are allowed
			if (state == ExecutionState.RUNNING ||
				state == ExecutionState.FINISHED ||
				state == ExecutionState.SCHEDULED ||
				state == ExecutionState.DEPLOYING) {
				NettyShuffleDescriptor defaultSdd;

				// Create local or remote channels
				assertTrue(localSdd instanceof NettyShuffleDescriptor);
				defaultSdd = (NettyShuffleDescriptor) localSdd;
				assertEquals(localPartitionId, defaultSdd.getResultPartitionID());
				assertTrue(defaultSdd.isLocalTo(consumerResourceID));

				assertTrue(remoteSdd instanceof NettyShuffleDescriptor);
				defaultSdd = (NettyShuffleDescriptor) remoteSdd;
				assertEquals(remotePartitionId, defaultSdd.getResultPartitionID());
				assertFalse(defaultSdd.isLocalTo(consumerResourceID));
				assertEquals(CONNECTION, defaultSdd.getConnectionId());
			} else {
				// Unknown (lazy deployment allowed)
				assertTrue(localSdd instanceof UnknownShuffleDescriptor);
				typedUnknownSdd = (UnknownShuffleDescriptor) localSdd;
				assertEquals(localPartitionId, typedUnknownSdd.getResultPartitionID());

				assertTrue(remoteSdd instanceof UnknownShuffleDescriptor);
				typedUnknownSdd = (UnknownShuffleDescriptor) remoteSdd;
				assertEquals(remotePartitionId, typedUnknownSdd.getResultPartitionID());
			}

			assertTrue(unknownSdd instanceof UnknownShuffleDescriptor);
			typedUnknownSdd = (UnknownShuffleDescriptor) unknownSdd;
			assertEquals(unknownPartitionId, typedUnknownSdd.getResultPartitionID());
		}
	}

	@Test
	public void testUnknownChannelWithoutLazyDeploymentThrows() throws Exception {
		ResultPartitionID unknownPartitionId = new ResultPartitionID();

		// This should work if lazy deployment is allowed
		ShuffleDescriptor unknownSdd = getConsumedPartitionSdd(
			unknownPartitionId,
			ExecutionState.CREATED,
			Collections.emptyMap(),
			true);

		assertTrue(unknownSdd instanceof UnknownShuffleDescriptor);
		UnknownShuffleDescriptor typedUnknownSdd = (UnknownShuffleDescriptor) unknownSdd;
		assertEquals(unknownPartitionId, typedUnknownSdd.getResultPartitionID());

		try {
			// Fail if lazy deployment is *not* allowed
			getConsumedPartitionSdd(
				unknownPartitionId,
				ExecutionState.CREATED,
				Collections.emptyMap(),
				false);
			fail("Did not throw expected ExecutionGraphException");
		} catch (ExecutionGraphException ignored) {
		}
	}

	private static ShuffleDescriptor getConsumedPartitionSdd(
			ResultPartitionID id,
			ExecutionState state,
			Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions,
			boolean allowLazyDeployment) throws ExecutionGraphException {
		return TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDeploymentDescriptor(
			id,
			ResultPartitionType.PIPELINED,
			true,
			state,
			allowLazyDeployment,
			producedPartitions);
	}

	private static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
			ResultPartitionID id,
			ResourceID location) throws ExecutionException, InterruptedException {
		ProducerDescriptor producerDescriptor =
			new ProducerDescriptor(location, CONNECTION.getAddress(), id.getProducerId());
		PartitionDescriptor partitionDescriptor = new PartitionDescriptor(
			new IntermediateDataSetID(),
			id.getPartitionId(),
			ResultPartitionType.PIPELINED,
			1,
			0);
		ShuffleDescriptor shuffleDescriptor =
			NettyShuffleMaster.INSTANCE.registerPartitionWithProducer(
				partitionDescriptor,
				producerDescriptor).get();
		return new ResultPartitionDeploymentDescriptor(
			partitionDescriptor,
			shuffleDescriptor,
			1,
			true);
	}
}
