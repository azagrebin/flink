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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.DualKeyLinkedMap;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

class SharedSlot implements SlotOwner, PhysicalSlot.Payload {
	private static final Logger LOG = LoggerFactory.getLogger(SharedSlot.class);

	private final SlotRequestId physicalSlotRequestId;

	private final ResourceProfile physicalSlotResourceProfile;

	private final ExecutionSlotSharingGroup executionSlotSharingGroup;

	private final CompletableFuture<PhysicalSlot> slotContextFuture;

	private final DualKeyLinkedMap<ExecutionVertexID, SlotRequestId, CompletableFuture<SingleLogicalSlot>> requestedLogicalSlots;

	private final boolean slotWillBeOccupiedIndefinitely;

	private final Consumer<ExecutionSlotSharingGroup> releaseCallback;

	SharedSlot(
			SlotRequestId physicalSlotRequestId,
			ResourceProfile physicalSlotResourceProfile,
			ExecutionSlotSharingGroup executionSlotSharingGroup,
			CompletableFuture<PhysicalSlot> slotContextFuture,
			boolean slotWillBeOccupiedIndefinitely,
			Consumer<ExecutionSlotSharingGroup> releaseCallback) {
		this.physicalSlotRequestId = physicalSlotRequestId;
		this.physicalSlotResourceProfile = physicalSlotResourceProfile;
		this.executionSlotSharingGroup = executionSlotSharingGroup;
		this.slotContextFuture = slotContextFuture.thenApply(physicalSlot -> {
			Preconditions.checkState(
				physicalSlot.tryAssignPayload(this),
				"Unexpected physical slot payload assignment failure!");
			return physicalSlot;
		});
		this.requestedLogicalSlots = new DualKeyLinkedMap<>(executionSlotSharingGroup.getExecutionVertexIds().size());
		this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
		this.releaseCallback = releaseCallback;
	}

	SlotRequestId getPhysicalSlotRequestId() {
		return physicalSlotRequestId;
	}

	ResourceProfile getPhysicalSlotResourceProfile() {
		return physicalSlotResourceProfile;
	}

	CompletableFuture<PhysicalSlot> getSlotContextFuture() {
		return slotContextFuture;
	}

	CompletableFuture<LogicalSlot> allocateLogicalSlot(ExecutionVertexID executionVertexId) {
		Preconditions.checkArgument(executionSlotSharingGroup.getExecutionVertexIds().contains(executionVertexId));
		CompletableFuture<SingleLogicalSlot> logicalSlotFuture = requestedLogicalSlots.getValueByKeyA(executionVertexId);
		if (logicalSlotFuture != null) {
			LOG.debug("Request for {} already exists", getLogicalSlotString(executionVertexId));
		} else {
			logicalSlotFuture = allocateNonExistentLogicalSlot(executionVertexId);
		}
		return logicalSlotFuture.thenApply(Function.identity());
	}

	private CompletableFuture<SingleLogicalSlot> allocateNonExistentLogicalSlot(ExecutionVertexID executionVertexId) {
		CompletableFuture<SingleLogicalSlot> logicalSlotFuture;
		SlotRequestId logicalSlotRequestId = new SlotRequestId();
		String logMessageBase = getLogicalSlotString(logicalSlotRequestId, executionVertexId);
		LOG.debug("Request a {}", logMessageBase);

		logicalSlotFuture = slotContextFuture
			.thenApply(physicalSlot -> {
				LOG.debug("Allocated {}", logMessageBase);
				return createLogicalSlot(physicalSlot, logicalSlotRequestId);
			});
		requestedLogicalSlots.put(executionVertexId, logicalSlotRequestId, logicalSlotFuture);

		// If the physical slot request fails (slotContextFuture), it will also fail the logicalSlotFuture.
		// Therefore, the next `exceptionally` callback will cancelLogicalSlotRequest and do the cleanup
		// in requestedLogicalSlots and eventually in sharedSlots
		logicalSlotFuture.exceptionally(cause -> {
			LOG.debug("Failed {}", logMessageBase);
			removeLogicalSlotRequest(logicalSlotRequestId);
			return null;
		});
		return logicalSlotFuture;
	}

	private SingleLogicalSlot createLogicalSlot(PhysicalSlot physicalSlot, SlotRequestId logicalSlotRequestId) {
		return new SingleLogicalSlot(
			logicalSlotRequestId,
			physicalSlot,
			null,
			Locality.UNKNOWN,
			this,
			slotWillBeOccupiedIndefinitely);
	}

	void cancelLogicalSlotRequest(ExecutionVertexID executionVertexID, @Nullable Throwable cause) {
		CompletableFuture<SingleLogicalSlot> logicalSlotFuture = requestedLogicalSlots.getValueByKeyA(executionVertexID);
		SlotRequestId logicalSlotRequestId = requestedLogicalSlots.getKeyBByKeyA(executionVertexID);
		if (logicalSlotFuture != null) {
			LOG.debug("Cancel {} from {}", getLogicalSlotString(logicalSlotRequestId), executionVertexID);
			if (cause == null) {
				logicalSlotFuture.cancel(false);
			} else {
				logicalSlotFuture.completeExceptionally(cause);
			}
		} else {
			LOG.debug("No SlotExecutionVertexAssignment for logical {} from physical {}}", logicalSlotRequestId, physicalSlotRequestId);
		}
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		removeLogicalSlotRequest(logicalSlot.getSlotRequestId());
	}

	private void removeLogicalSlotRequest(SlotRequestId logicalSlotRequestId) {
		requestedLogicalSlots.removeKeyB(logicalSlotRequestId);
		if (requestedLogicalSlots.values().isEmpty()) {
			releaseCallback.accept(executionSlotSharingGroup);
		}
	}

	@Override
	public void release(Throwable cause) {
		Preconditions.checkState(
			slotContextFuture.isDone(),
			"Releasing of the shared slot is expected only from its successfully allocated physical slot ({})",
			physicalSlotRequestId);
		for (ExecutionVertexID executionVertexId : requestedLogicalSlots.keySetA()) {
			LOG.debug("Release {}", getLogicalSlotString(executionVertexId));
			CompletableFuture<SingleLogicalSlot> logicalSlotFuture =
				requestedLogicalSlots.getValueByKeyA(executionVertexId);
			Preconditions.checkNotNull(logicalSlotFuture);
			Preconditions.checkState(
				logicalSlotFuture.isDone(),
				"Logical slot future must already done when release call comes from the successfully allocated physical slot ({})",
				physicalSlotRequestId);
			logicalSlotFuture.thenAccept(logicalSlot -> logicalSlot.release(cause));
		}
		requestedLogicalSlots.clear();
		releaseCallback.accept(executionSlotSharingGroup);
	}

	@Override
	public boolean willOccupySlotIndefinitely() {
		return slotWillBeOccupiedIndefinitely;
	}

	private String getLogicalSlotString(SlotRequestId logicalSlotRequestId) {
		return getLogicalSlotString(logicalSlotRequestId, requestedLogicalSlots.getKeyAByKeyB(logicalSlotRequestId));
	}

	private String getLogicalSlotString(ExecutionVertexID executionVertexId) {
		return getLogicalSlotString(requestedLogicalSlots.getKeyBByKeyA(executionVertexId), executionVertexId);
	}

	private String getLogicalSlotString(SlotRequestId logicalSlotRequestId, ExecutionVertexID executionVertexId) {
		return String.format(
			"logical slot (%s) for execution vertex (id %s) from the physical slot (%s)",
			logicalSlotRequestId,
			executionVertexId,
			physicalSlotRequestId);
	}
}
