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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.DefaultShuffleDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.PartitionShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDeploymentDescriptor;

import java.net.InetSocketAddress;

/**
 * Common utility methods to test Shuffle implementation.
 */
public class ShuffleTestUtil {
	private ShuffleTestUtil() {
	}

	public static DefaultShuffleDeploymentDescriptor createSddWithLocalConnection(
		IntermediateResultPartitionID partitionId, ResourceID producerLocation, int dataPort) {

		ExecutionAttemptID producerExecutionId = new ExecutionAttemptID();
		ResultPartitionID resultPartitionID = new ResultPartitionID(partitionId, producerExecutionId);
		return createSddWithLocalConnection(resultPartitionID, producerLocation, dataPort);
	}

	public static DefaultShuffleDeploymentDescriptor createSddWithLocalConnection(
		ResultPartitionID resultPartitionID, ResourceID producerLocation, int dataPort) {

		InetSocketAddress producerAddress = new InetSocketAddress("localhost", dataPort);
		return new DefaultShuffleDeploymentDescriptor(producerLocation, producerAddress, resultPartitionID, 0);
	}

	public static ResultPartitionDeploymentDescriptor createResultPartitionDeploymentDescriptor(
		DefaultShuffleDeploymentDescriptor sdd) {

		PartitionShuffleDescriptor psd = new PartitionShuffleDescriptor(
			new IntermediateDataSetID(), sdd.getResultPartitionID().getPartitionId(),
			ResultPartitionType.PIPELINED, 1, 1, 0);
		return new ResultPartitionDeploymentDescriptor(psd, sdd,  true);
	}

	public static InputGateDeploymentDescriptor createInputGateDeploymentDescriptor(
		ShuffleDeploymentDescriptor sdd, ResourceID consumerLocation) {

		return new InputGateDeploymentDescriptor(new IntermediateDataSetID(), ResultPartitionType.PIPELINED, 0,
			new ShuffleDeploymentDescriptor[] { sdd }, consumerLocation);
	}

	public static PartitionInfo createLocalPartitionInfo(ResultPartitionID resultPartitionID) {
		ResourceID location = new ResourceID("local");
		ShuffleDeploymentDescriptor sdd =
			createSddWithLocalConnection(resultPartitionID, location, 10000);
		return createPartitionInfo(sdd, location);
	}

	public static PartitionInfo createPartitionInfo(ShuffleDeploymentDescriptor sdd, ResourceID consumerLocation) {
		return new PartitionInfo(new IntermediateDataSetID(), consumerLocation, sdd);
	}
}
