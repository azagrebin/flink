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
import org.apache.flink.runtime.shuffle.DefaultShuffleDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.DefaultShuffleMaster;
import org.apache.flink.runtime.shuffle.PartitionShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ProducerShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDeploymentDescriptor;

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
 * Tests for the {@link ShuffleDeploymentDescriptor}.
 */
public class ShuffleDeploymentDescriptorTest {
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
			addPartition(localPartitionId, consumerResourceID, producedPartitions);

			ResultPartitionID remotePartitionId = new ResultPartitionID();
			addPartition(remotePartitionId, ResourceID.generate(), producedPartitions);

			ResultPartitionID unknownPartitionId = new ResultPartitionID();

			ShuffleDeploymentDescriptor localSdd =
				getConsumedPartitionSdd(localPartitionId, state, producedPartitions);
			ShuffleDeploymentDescriptor remoteSdd =
				getConsumedPartitionSdd(remotePartitionId, state, producedPartitions);
			ShuffleDeploymentDescriptor unknownSdd =
				getConsumedPartitionSdd(unknownPartitionId, state, producedPartitions);

			UnknownShuffleDeploymentDescriptor typedUnknownSdd;

			// These states are allowed
			if (state == ExecutionState.RUNNING || state == ExecutionState.FINISHED ||
				state == ExecutionState.SCHEDULED || state == ExecutionState.DEPLOYING) {
				DefaultShuffleDeploymentDescriptor defaultSdd;

				// Create local or remote channels
				assertTrue(localSdd instanceof DefaultShuffleDeploymentDescriptor);
				defaultSdd = (DefaultShuffleDeploymentDescriptor) localSdd;
				assertEquals(localPartitionId, defaultSdd.getResultPartitionID());
				assertTrue(defaultSdd.isLocalTo(consumerResourceID));

				assertTrue(remoteSdd instanceof DefaultShuffleDeploymentDescriptor);
				defaultSdd = (DefaultShuffleDeploymentDescriptor) remoteSdd;
				assertEquals(remotePartitionId, defaultSdd.getResultPartitionID());
				assertFalse(defaultSdd.isLocalTo(consumerResourceID));
				assertEquals(CONNECTION, defaultSdd.getConnectionId());
			} else {
				// Unknown (lazy deployment allowed)
				assertTrue(localSdd instanceof UnknownShuffleDeploymentDescriptor);
				typedUnknownSdd = (UnknownShuffleDeploymentDescriptor) localSdd;
				assertEquals(localPartitionId, typedUnknownSdd.getResultPartitionID());

				assertTrue(remoteSdd instanceof UnknownShuffleDeploymentDescriptor);
				typedUnknownSdd = (UnknownShuffleDeploymentDescriptor) remoteSdd;
				assertEquals(remotePartitionId, typedUnknownSdd.getResultPartitionID());
			}

			assertTrue(unknownSdd instanceof UnknownShuffleDeploymentDescriptor);
			typedUnknownSdd = (UnknownShuffleDeploymentDescriptor) unknownSdd;
			assertEquals(unknownPartitionId, typedUnknownSdd.getResultPartitionID());
		}
	}

	@Test
	public void testUnknownChannelWithoutLazyDeploymentThrows() throws Exception {
		ResultPartitionID unknownPartitionId = new ResultPartitionID();

		// This should work if lazy deployment is allowed
		ShuffleDeploymentDescriptor unknownSdd = getConsumedPartitionSdd(
			unknownPartitionId, ExecutionState.CREATED, Collections.emptyMap(), true);

		assertTrue(unknownSdd instanceof UnknownShuffleDeploymentDescriptor);
		UnknownShuffleDeploymentDescriptor typedUnknownSdd = (UnknownShuffleDeploymentDescriptor) unknownSdd;
		assertEquals(unknownPartitionId, typedUnknownSdd.getResultPartitionID());

		try {
			// Fail if lazy deployment is *not* allowed
			getConsumedPartitionSdd(
				unknownPartitionId, ExecutionState.CREATED, Collections.emptyMap(), false);
			fail("Did not throw expected ExecutionGraphException");
		} catch (ExecutionGraphException ignored) {
		}
	}

	private static ShuffleDeploymentDescriptor getConsumedPartitionSdd(
		ResultPartitionID id,
		ExecutionState state,
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions)
		throws ExecutionGraphException {

		return getConsumedPartitionSdd(id, state, producedPartitions, true);
	}

	private static ShuffleDeploymentDescriptor getConsumedPartitionSdd(
		ResultPartitionID id,
		ExecutionState state,
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions,
		boolean allowLazyDeployment)
		throws ExecutionGraphException {

		return TaskDeploymentDescriptorFactory.getConsumedPartitionSdd(
			id, ResultPartitionType.PIPELINED, true, state, allowLazyDeployment, producedPartitions);
	}

	private static void addPartition(
		ResultPartitionID id,
		ResourceID location,
		Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions)
		throws ExecutionException, InterruptedException {

		ProducerShuffleDescriptor prsd =
			new ProducerShuffleDescriptor(location, CONNECTION.getAddress(), id.getProducerId());
		PartitionShuffleDescriptor psd = new PartitionShuffleDescriptor(
			new IntermediateDataSetID(), id.getPartitionId(), ResultPartitionType.PIPELINED,
			1, 1, 0);
		ShuffleDeploymentDescriptor sdd =
			DefaultShuffleMaster.getInstance().registerPartitionWithProducer(psd, prsd).get();
		ResultPartitionDeploymentDescriptor rdd =
			new ResultPartitionDeploymentDescriptor(psd, sdd, true);
		producedPartitions.put(id.getPartitionId(), rdd);
	}
}
