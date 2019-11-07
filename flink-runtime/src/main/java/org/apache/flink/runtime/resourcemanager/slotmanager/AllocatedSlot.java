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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.taskexecutor.SlotStatus;

class AllocatedSlot {
	/** The resource profile of this slot. */
	private final ResourceProfile resourceProfile;

	/** Allocation id for which this slot has been allocated. */
	private final AllocationID allocationId;

	/** Allocation id for which this slot has been allocated. */
	private final JobID jobId;

	AllocatedSlot(ResourceProfile resourceProfile, AllocationID allocationId, JobID jobId) {
		this.resourceProfile = resourceProfile;
		this.allocationId = allocationId;
		this.jobId = jobId;
	}

	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public JobID getJobId() {
		return jobId;
	}

	public static AllocatedSlot fromPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
		return new AllocatedSlot(
			pendingSlotRequest.getResourceProfile(),
			pendingSlotRequest.getAllocationId(),
			pendingSlotRequest.getJobId());
	}

	public static AllocatedSlot fromSlotStatus(SlotStatus slotStatus) {
		return new AllocatedSlot(
			slotStatus.getResourceProfile(),
			slotStatus.getAllocationID(),
			slotStatus.getJobID());
	}
}
