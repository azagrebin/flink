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
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

abstract class AbstractTaskManagerResource {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskManagerResource.class);

	/** The resource profile of total task manager resources. */
	private final ResourceProfile totalResourceProfile;

	/** The resource profile of available resources. */
	private ResourceProfile availableResourceProfile;

	/** The resource profile of default slot for this task manager. */
	private final ResourceProfile defaultSlotResourceProfile;

	/** Assigned slot requests if there are currently any ongoing requests. */
	protected final Map<AllocationID, PendingSlotRequest> pendingSlotRequests;

	AbstractTaskManagerResource(ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile) {
		this.totalResourceProfile = totalResourceProfile;
		this.availableResourceProfile = totalResourceProfile;
		this.defaultSlotResourceProfile = defaultSlotResourceProfile;
		this.pendingSlotRequests = new HashMap<>();
	}

	public boolean inTotalSameAs(ResourceProfile resourceProfile) {
		return totalResourceProfile.equals(resourceProfile);
	}

	public Optional<PendingSlotRequest> getPendingSlotRequest(AllocationID allocationID) {
		return Optional.ofNullable(pendingSlotRequests.getOrDefault(allocationID, null));
	}

	public boolean canBeAssignedPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
		ResourceProfile resourceRequest = getRequestedResourceRequest(pendingSlotRequest);
		return availableResourceProfile.isMatching(resourceRequest);
	}

	public void assignPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
		Preconditions.checkArgument(canBeAssignedPendingSlotRequest(pendingSlotRequest));
		pendingSlotRequests.put(pendingSlotRequest.getAllocationId(), pendingSlotRequest);
		allocateResource(getRequestedResourceRequest(pendingSlotRequest));
		pendingSlotRequest.assignTaskManagerPendingResource(this);
	}

	private ResourceProfile getRequestedResourceRequest(PendingSlotRequest pendingSlotRequest) {
		ResourceProfile resourceRequest = pendingSlotRequest.getResourceProfile();
		return resourceRequest.equals(ResourceProfile.ANY) ? defaultSlotResourceProfile : resourceRequest;
	}

	public void rejectPendingSlotRequest(PendingSlotRequest pendingSlotRequest, Throwable cause) {
		pendingSlotRequest.unassignTaskManagerPendingResource();

		CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

		if (null != request) {
			request.completeExceptionally(new SlotAllocationException(cause));
		} else {
			LOG.debug("Cannot reject pending slot request {}, since no request has been sent.", pendingSlotRequest.getAllocationId());
		}
	}

	public boolean unassignPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
		return unassignPendingSlotRequest(pendingSlotRequest, true);
	}

	protected boolean unassignPendingSlotRequest(PendingSlotRequest pendingSlotRequest, boolean freeResouce) {
		if (pendingSlotRequests.remove(pendingSlotRequest.getAllocationId()) != null) {
			if (freeResouce) {
				freeResource(getRequestedResourceRequest(pendingSlotRequest));
			}
			Preconditions.checkState(totalResourceProfile.isMatching(availableResourceProfile));
			return true;
		}
		return false;
	}

	protected void allocateResource(ResourceProfile resourceProfile) {
		availableResourceProfile = availableResourceProfile.subtract(resourceProfile);
	}

	protected void freeResource(ResourceProfile resourceProfile) {
		availableResourceProfile = availableResourceProfile.merge(resourceProfile);
	}

	public boolean isIdle() {
		return totalResourceProfile.equals(availableResourceProfile);
	}

	public boolean canBePotentiallyAllocated(ResourceProfile resourceProfile) {
		return totalResourceProfile.isMatching(resourceProfile);
	}
}
