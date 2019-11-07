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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskexecutor.SlotStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** . */
public class TaskManagerAvailableResource extends AbstractTaskManagerResource {
	private final Map<AllocationID, AllocatedSlot> allocatedSlots;

	public TaskManagerAvailableResource(
			ResourceProfile totalResourceProfile,
			ResourceProfile defaultSlotResourceProfile) {
		super(totalResourceProfile, defaultSlotResourceProfile);
		this.allocatedSlots = new HashMap<>();
	}

	public void pendingRequestAllocated(PendingSlotRequest pendingSlotRequest) {
		if (unassignPendingSlotRequest(pendingSlotRequest, false)) {
			allocatedSlots.put(
				pendingSlotRequest.getAllocationId(),
				AllocatedSlot.fromPendingSlotRequest(pendingSlotRequest));
		}
	}

	public List<PendingSlotRequest> updateWithSlotReport(Iterable<SlotStatus> slotReport) {
		List<PendingSlotRequest> fulfilledPendingSlotRequests = new ArrayList<>();
		Collection<AllocationID> allocatedReportSlots = new HashSet<>();
		for (SlotStatus slotStatus : slotReport) {
			AllocationID allocationID = slotStatus.getAllocationID();
			if (allocationID != null) {
				allocatedReportSlots.add(allocationID);
				updateAllocation(slotStatus).ifPresent(fulfilledPendingSlotRequests::add);
			}
		}

		boolean resourceFreed = false;
		for (AllocatedSlot allocatedSlot : new ArrayList<>(allocatedSlots.values())) {
			if (!allocatedReportSlots.contains(allocatedSlot.getAllocationId())) {
				freeResource(allocatedSlot.getResourceProfile());
				allocatedSlots.remove(allocatedSlot.getAllocationId());
				resourceFreed = true; // TODO: handleFreeResource
			}
		}
		return fulfilledPendingSlotRequests;
	}

	private Optional<PendingSlotRequest> updateAllocation(SlotStatus slotStatus) {
		AllocationID allocationID = slotStatus.getAllocationID();
		AllocatedSlot allocatedSlot = allocatedSlots.get(allocationID);
		if (allocatedSlot == null) {
			Optional<PendingSlotRequest> pendingSlotRequest = getPendingSlotRequest(allocationID);
			if (pendingSlotRequest.isPresent()) {
				CompletableFuture<Acknowledge> requestFuture = pendingSlotRequest.get().getRequestFuture();
				if (requestFuture != null) {
					requestFuture.cancel(false);
				}
				pendingRequestAllocated(pendingSlotRequest.get());
			} else {
				allocatedSlots.put(allocationID, AllocatedSlot.fromSlotStatus(slotStatus));
				allocateResource(slotStatus.getResourceProfile());
			}
			return pendingSlotRequest;
		}
		return Optional.empty();
	}

	public static class SlotUpdateReport {

	}
}
