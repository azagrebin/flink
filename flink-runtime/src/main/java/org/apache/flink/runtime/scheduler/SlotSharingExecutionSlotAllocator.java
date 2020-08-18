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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Allocates {@link LogicalSlot}s from physical shared slots.
 *
 * <p>The allocator maintains a shared slot for each {@link ExecutionSlotSharingGroup}.
 * It allocates a physical slot for the shared slot and then allocates logical slots from it for scheduled tasks.
 * The physical slot is lazily allocated for a shared slot, upon any hosted subtask asking for the shared slot.
 * Each subsequent sharing subtask allocates a logical slot from the existing shared slot. The shared/physical slot
 * can be released only if all the requested logical slots are released or canceled.
 */
class SlotSharingExecutionSlotAllocator implements ExecutionSlotAllocator {
	private static final Logger LOG = LoggerFactory.getLogger(SlotSharingExecutionSlotAllocator.class);

	private final PhysicalSlotProvider slotProvider;

	private final boolean slotWillBeOccupiedIndefinitely;

	private final SlotSharingStrategy slotSharingStrategy;

	private final Map<ExecutionSlotSharingGroup, SharedSlot> sharedSlots;

	private final SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory;

	private final PhysicalSlotRequestBulkChecker bulkChecker;

	private final Time allocationTimeout;

	private final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever;

	SlotSharingExecutionSlotAllocator(
			PhysicalSlotProvider slotProvider,
			boolean slotWillBeOccupiedIndefinitely,
			SlotSharingStrategy slotSharingStrategy,
			SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory,
			PhysicalSlotRequestBulkChecker bulkChecker,
			Time allocationTimeout,
			Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {
		this.slotProvider = checkNotNull(slotProvider);
		this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
		this.slotSharingStrategy = checkNotNull(slotSharingStrategy);
		this.sharedSlotProfileRetrieverFactory = checkNotNull(sharedSlotProfileRetrieverFactory);
		this.bulkChecker = checkNotNull(bulkChecker);
		this.allocationTimeout = checkNotNull(allocationTimeout);
		this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
		this.sharedSlots = new IdentityHashMap<>();
	}

	/**
	 * Creates logical {@link SlotExecutionVertexAssignment}s from physical shared slots.
	 *
	 * <p>The allocation has the following steps:
	 * <ol>
	 *  <li>Map the executions to {@link ExecutionSlotSharingGroup}s using {@link SlotSharingStrategy}</li>
	 *  <li>Check which {@link ExecutionSlotSharingGroup}s already have shared slot</li>
	 *  <li>For all involved {@link ExecutionSlotSharingGroup}s which do not have a shared slot yet:</li>
	 *  <li>Create a {@link SlotProfile} future using {@link SharedSlotProfileRetriever} and then</li>
	 *  <li>Allocate a physical slot from the {@link PhysicalSlotProvider}</li>
	 *  <li>Create a shared slot based on the returned physical slot futures</li>
	 *  <li>Allocate logical slot futures for the executions from all corresponding shared slots.</li>
	 *  <li>If a physical slot request fails, associated logical slot requests are canceled within the shared slot</li>
	 *  <li>Generate {@link SlotExecutionVertexAssignment}s based on the logical slot futures and returns the results.</li>
	 * </ol>
	 *
	 * @param executionVertexSchedulingRequirements the requirements for scheduling the executions.
	 */
	@Override
	public List<SlotExecutionVertexAssignment> allocateSlotsFor(
			List<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {
		List<ExecutionVertexID> executionVertexIds = executionVertexSchedulingRequirements
			.stream()
			.map(ExecutionVertexSchedulingRequirements::getExecutionVertexId)
			.collect(Collectors.toList());

		SharedSlotProfileRetriever sharedSlotProfileRetriever = sharedSlotProfileRetrieverFactory
			.createFromBulk(new HashSet<>(executionVertexIds));
		Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup = executionVertexIds
			.stream()
			.collect(Collectors.groupingBy(slotSharingStrategy::getExecutionSlotSharingGroup));
		Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = executionsByGroup
			.entrySet()
			.stream()
			.flatMap(entry -> allocateLogicalSlotsFromSharedSlot(sharedSlotProfileRetriever, entry.getKey(), entry.getValue()))
			.collect(Collectors.toMap(SlotExecutionVertexAssignment::getExecutionVertexId, a -> a));

		bulkChecker.schedulePendingRequestBulkTimeoutCheck(createBulk(executionsByGroup), allocationTimeout);

		return executionVertexIds.stream().map(assignments::get).collect(Collectors.toList());
	}

	@Override
	public void cancel(ExecutionVertexID executionVertexId) {
		ExecutionSlotSharingGroup executionSlotSharingGroup =
			slotSharingStrategy.getExecutionSlotSharingGroup(executionVertexId);
		checkNotNull(
			executionSlotSharingGroup,
			"There is no ExecutionSlotSharingGroup for ExecutionVertexID " + executionVertexId);
		SharedSlot slot = sharedSlots.get(executionSlotSharingGroup);
		if (slot != null) {
			slot.cancelLogicalSlotRequest(executionVertexId);
		} else {
			LOG.debug("There is no slot for ExecutionSlotSharingGroup of ExecutionVertexID {}", executionVertexId);
		}
	}

	private Stream<SlotExecutionVertexAssignment> allocateLogicalSlotsFromSharedSlot(
			SharedSlotProfileRetriever sharedSlotProfileRetriever,
			ExecutionSlotSharingGroup executionSlotSharingGroup,
			Collection<ExecutionVertexID> executions) {
		SharedSlot sharedSlot = getOrAllocateSharedSlot(executionSlotSharingGroup, sharedSlotProfileRetriever);
		return executions
			.stream()
			.map(execution -> new SlotExecutionVertexAssignment(execution, sharedSlot.allocateLogicalSlot(execution)));
	}

	private SharedSlot getOrAllocateSharedSlot(
			ExecutionSlotSharingGroup executionSlotSharingGroup,
			SharedSlotProfileRetriever sharedSlotProfileRetriever) {
		return sharedSlots
			.computeIfAbsent(executionSlotSharingGroup, group -> {
				SlotRequestId physicalSlotRequestId = new SlotRequestId();
				ResourceProfile physicalSlotResourceProfile = getPhysicalSlotResourceProfile(group);
				CompletableFuture<PhysicalSlot> physicalSlotFuture = sharedSlotProfileRetriever
					.getSlotProfileFuture(group, physicalSlotResourceProfile)
					.thenCompose(slotProfile -> slotProvider.allocatePhysicalSlot(
						new PhysicalSlotRequest(physicalSlotRequestId, slotProfile, slotWillBeOccupiedIndefinitely)))
					.thenApply(PhysicalSlotRequest.Result::getPhysicalSlot);
				return new SharedSlot(
					physicalSlotRequestId,
					physicalSlotResourceProfile,
					group,
					physicalSlotFuture,
					slotWillBeOccupiedIndefinitely,
					this::releaseSharedSlot);
			});
	}

	private void releaseSharedSlot(ExecutionSlotSharingGroup executionSlotSharingGroup) {
		SharedSlot slot = sharedSlots.remove(executionSlotSharingGroup);
		if (slot != null) {
			slotProvider.cancelSlotRequest(
				slot.getPhysicalSlotRequestId(),
				new FlinkException("Slot is being returned from SlotSharingExecutionSlotAllocator."));
		} else {
			LOG.debug("There is no slot for ExecutionSlotSharingGroup {} to release", executionSlotSharingGroup);
		}
	}

	private ResourceProfile getPhysicalSlotResourceProfile(ExecutionSlotSharingGroup executionSlotSharingGroup) {
		return executionSlotSharingGroup
			.getExecutionVertexIds()
			.stream()
			.reduce(ResourceProfile.ZERO, (r, e) -> r.merge(resourceProfileRetriever.apply(e)), ResourceProfile::merge);
	}

	private SharingPhysicalSlotRequestBulk createBulk(Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executions) {
		Map<ExecutionSlotSharingGroup, ResourceProfile> pendingRequests = executions
			.keySet()
			.stream()
			.collect(Collectors.toMap(
				group -> group,
				group -> sharedSlots.get(group).getPhysicalSlotResourceProfile()
			));
		Map<ExecutionSlotSharingGroup, AllocationID> fulfilledRequests = new HashMap<>();
		SharingPhysicalSlotRequestBulk bulk = new SharingPhysicalSlotRequestBulk(
			executions,
			pendingRequests,
			fulfilledRequests,
			(group, executionVertexIds) -> {
				for (ExecutionVertexID execution : executionVertexIds) {
					sharedSlots.get(group).cancelLogicalSlotRequest(execution);
				}
			});
		registerPhysicalSlotRequestBulkCallbacks(executions.keySet(), bulk);
		return bulk;
	}

	private void registerPhysicalSlotRequestBulkCallbacks(
			Iterable<ExecutionSlotSharingGroup> executions,
			SharingPhysicalSlotRequestBulk bulk) {
		for (ExecutionSlotSharingGroup group : executions) {
			CompletableFuture<PhysicalSlot> slotContextFuture = sharedSlots.get(group).getSlotContextFuture();
			slotContextFuture.thenAccept(physicalSlot -> bulk.markFulfilled(group, physicalSlot.getAllocationId()));
			slotContextFuture.exceptionally(t -> {
				// clear the bulk to stop the fulfil-ability check
				bulk.clear();
				return null;
			});
		}
	}
}
