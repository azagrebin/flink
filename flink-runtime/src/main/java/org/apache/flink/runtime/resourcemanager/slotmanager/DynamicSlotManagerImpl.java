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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnfulfillableSlotRequestException;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of {@link SlotManager} with dynamic slot allocation.
 */
public class DynamicSlotManagerImpl implements SlotManager {
	private static final Logger LOG = LoggerFactory.getLogger(DynamicSlotManagerImpl.class);

	/** Scheduled executor for timeouts. */
	private final ScheduledExecutor scheduledExecutor;

	/** Timeout for slot requests to the task manager. */
	private final Time taskManagerRequestTimeout;

	/** Timeout after which an allocation is discarded. */
	private final Time slotRequestTimeout;

	/** Timeout after which an unused TaskManager is released. */
	private final Time taskManagerTimeout;

	/** All currently registered task managers. */
	private final Map<InstanceID, TaskManagerRegistration> taskManagerRegistrations;

	//private final HashMap<TaskManagerSlotId, PendingTaskManagerSlot> pendingSlots; // ---> pendingResources
	private final Map<TaskManagerPendingId, TaskManagerPendingResource> pendingResources;

	/** Map of pending/unfulfilled slot allocation requests which are not assigned. */
	private final HashMap<AllocationID, PendingSlotRequest> pendingSlotRequests;

	/** ResourceManager's id. */
	private ResourceManagerId resourceManagerId;

	/** Executor for future callbacks which have to be "synchronized". */
	private Executor mainThreadExecutor;

	/** Callbacks for resource (de-)allocations. */
	private ResourceActions resourceActions;

	private ScheduledFuture<?> taskManagerTimeoutCheck;

	private ScheduledFuture<?> slotRequestTimeoutCheck;

	/** True iff the component has been started. */
	private boolean started;

	/** Release task executor only when each produced result partition is either consumed or failed. */
	private final boolean waitResultConsumedBeforeRelease;

	/**
	 * If true, fail unfulfillable slot requests immediately. Otherwise, allow unfulfillable request to pend.
	 * A slot request is considered unfulfillable if it cannot be fulfilled by neither a slot that is already registered
	 * (including allocated ones) nor a pending slot that the {@link ResourceActions} can allocate.
	 * */
	private boolean failUnfulfillableRequest = true;

	public DynamicSlotManagerImpl(
			ScheduledExecutor scheduledExecutor,
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout,
			boolean waitResultConsumedBeforeRelease) {

		this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
		this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.taskManagerTimeout = Preconditions.checkNotNull(taskManagerTimeout);
		this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;

		taskManagerRegistrations = new HashMap<>();
		pendingSlotRequests = new HashMap<>();
		pendingResources = new HashMap<>();

		resourceManagerId = null;
		resourceActions = null;
		mainThreadExecutor = null;
		taskManagerTimeoutCheck = null;
		slotRequestTimeoutCheck = null;

		started = false;
	}

	@Override
	public int getNumberRegisteredSlots() {
		return 0;
	}

	@Override
	public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);
		return taskManagerRegistration != null ? taskManagerRegistration.getNumberRegisteredSlots() : 0;
	}

	@Override
	public int getNumberFreeSlots() {
		return 0;
	}

	@Override
	public int getNumberFreeSlotsOf(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);
		return taskManagerRegistration != null ? taskManagerRegistration.getNumberFreeSlots() : 0;
	}

	@Override
	public int getNumberPendingTaskManagerSlots() {
		return 0;
	}

	@Override
	public int getNumberPendingSlotRequests() {
		return 0;
	}

	// ---------------------------------------------------------------------------------------------
	// Component lifecycle methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Starts the slot manager with the given leader id and resource manager actions.
	 *
	 * @param newResourceManagerId to use for communication with the task managers
	 * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
	 * @param newResourceActions to use for resource (de-)allocations
	 */
	@Override
	public void start(
			ResourceManagerId newResourceManagerId,
			Executor newMainThreadExecutor,
			ResourceActions newResourceActions) {
		LOG.info("Starting the SlotManager.");

		this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
		mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
		resourceActions = Preconditions.checkNotNull(newResourceActions);

		started = true;

		taskManagerTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				this::checkTaskManagerTimeouts),
			0L,
			taskManagerTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		slotRequestTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				this::checkSlotRequestTimeouts),
			0L,
			slotRequestTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);
	}

	/**
	 * Suspends the component. This clears the internal state of the slot manager.
	 */
	@Override
	public void suspend() {
		LOG.info("Suspending the SlotManager.");

		// stop the timeout checks for the TaskManagers and the SlotRequests
		if (taskManagerTimeoutCheck != null) {
			taskManagerTimeoutCheck.cancel(false);
			taskManagerTimeoutCheck = null;
		}

		if (slotRequestTimeoutCheck != null) {
			slotRequestTimeoutCheck.cancel(false);
			slotRequestTimeoutCheck = null;
		}

		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			cancelPendingSlotRequest(pendingSlotRequest);
		}

		pendingSlotRequests.clear();

		Iterable<InstanceID> registeredTaskManagers = new ArrayList<>(taskManagerRegistrations.keySet());

		for (InstanceID registeredTaskManager : registeredTaskManagers) {
			unregisterTaskManager(registeredTaskManager, new SlotManagerException("The slot manager is being suspended."));
		}

		resourceManagerId = null;
		resourceActions = null;
		started = false;
	}

	/**
	 * Closes the slot manager.
	 *
	 * @throws Exception if the close operation fails
	 */
	@Override
	public void close() throws Exception {
		LOG.info("Closing the SlotManager.");

		suspend();
	}

	// ---------------------------------------------------------------------------------------------
	// Public API
	// ---------------------------------------------------------------------------------------------

	/**
	 * Requests a slot with the respective resource profile.
	 *
	 * @param slotRequest specifying the requested slot specs
	 * @return true if the slot request was registered; false if the request is a duplicate
	 * @throws ResourceManagerException if the slot request failed (e.g. not enough resources left)
	 */
	@Override
	public boolean registerSlotRequest(SlotRequest slotRequest) throws ResourceManagerException {
		checkInit();

		if (checkDuplicateRequest(slotRequest.getAllocationId())) {
			LOG.debug("Ignoring a duplicate slot request with allocation id {}.", slotRequest.getAllocationId());

			return false;
		} else {
			PendingSlotRequest pendingSlotRequest = new PendingSlotRequest(slotRequest);
			pendingSlotRequests.put(slotRequest.getAllocationId(), pendingSlotRequest);
			try {
				internalRequestSlot(pendingSlotRequest);
			} catch (ResourceManagerException e) {
				// requesting the slot failed --> remove pending slot request
				pendingSlotRequests.remove(slotRequest.getAllocationId());
				throw new ResourceManagerException("Could not fulfill slot request " + slotRequest.getAllocationId() + '.', e);
			}

			return true;
		}
	}

	/**
	 * Cancels and removes a pending slot request with the given allocation id. If there is no such
	 * pending request, then nothing is done.
	 *
	 * @param allocationId identifying the pending slot request
	 * @return True if a pending slot request was found; otherwise false
	 */
	@Override
	public boolean unregisterSlotRequest(AllocationID allocationId) {
		checkInit();

		PendingSlotRequest pendingSlotRequest = pendingSlotRequests.remove(allocationId);

		if (pendingSlotRequest != null) {
			LOG.debug("Cancel slot request {}.", allocationId);

			cancelPendingSlotRequest(pendingSlotRequest);

			return true;
		} else {
			LOG.debug("No pending slot request with allocation id {} found. Ignoring unregistration request.", allocationId);

			return false;
		}
	}

	/**
	 * Registers a new task manager at the slot manager. This will make the task managers slots
	 * known and, thus, available for allocation.
	 *
	 * @param taskExecutorConnection for the new task manager
	 * @param initialSlotReport for the new task manager
	 */
	@Override
	public void registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		checkInit();

		LOG.debug(
			"Registering TaskManager {} under {} at the SlotManager.",
			taskExecutorConnection.getResourceID(),
			taskExecutorConnection.getInstanceID());

		// we identify task managers by their instance id
		if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
			reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
		} else {
			// first register the TaskManager
			TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(
				taskExecutorConnection,
				new ArrayList<>(),
				new TaskManagerAvailableResource(
					initialSlotReport.getAvailableResource(),
					taskExecutorConnection.getDefaultResourceProfile()));

			taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);

			findExactlyMatchingPendingResource(initialSlotReport.getAvailableResource()).ifPresent(pendingResource -> {
				for (PendingSlotRequest pendingSlotRequest : pendingResource.pendingSlotRequests.values()) {
					allocateSlot(taskManagerRegistration, pendingSlotRequest);
				}
				pendingResources.remove(pendingResource.getId());
			});

			handleFreeResource(taskManagerRegistration);
		}
	}

	@Override
	public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
		checkInit();

		LOG.debug("Unregister TaskManager {} from the SlotManager.", instanceId);

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.remove(instanceId);

		if (null != taskManagerRegistration) {
			internalUnregisterTaskManager(taskManagerRegistration, cause);

			return true;
		} else {
			LOG.debug("There is no task manager registered with instance ID {}. Ignoring this message.", instanceId);

			return false;
		}
	}

	/**
	 * Reports the current slot allocations for a task manager identified by the given instance id.
	 *
	 * @param instanceId identifying the task manager for which to report the slot status
	 * @param slotReport containing the status for all of its slots
	 * @return true if the slot status has been updated successfully, otherwise false
	 */
	@Override
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		checkInit();
		LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);
		if (taskManagerRegistration != null) {
			for (PendingSlotRequest pendingSlotRequest : taskManagerRegistration.getAvailableResource().updateWithSlotReport(slotReport)) {
				pendingSlotRequests.remove(pendingSlotRequest.getAllocationId());
			}
			return true;
		} else {
			LOG.debug("Received slot report for unknown task manager with instance id {}. Ignoring this report.", instanceId);
			return false;
		}
	}

	/**
	 * Free the given slot from the given allocation. If the slot is still allocated by the given
	 * allocation id, then the slot will be marked as free and will be subject to new slot requests.
	 *
	 * @param slotId identifying the slot to free
	 * @param allocationId with which the slot is presumably allocated
	 */
	@Override
	public void freeSlot(SlotID slotId, AllocationID allocationId) {
		checkInit();

		TaskManagerSlot slot = slots.get(slotId);

		if (null != slot) {
			if (slot.getState() == TaskManagerSlot.State.ALLOCATED) {
				if (Objects.equals(allocationId, slot.getAllocationId())) {

					TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

					if (taskManagerRegistration == null) {
						throw new IllegalStateException("Trying to free a slot from a TaskManager " +
							slot.getInstanceId() + " which has not been registered.");
					}

					updateSlotState(slot, taskManagerRegistration, null, null);
				} else {
					LOG.debug("Received request to free slot {} with expected allocation id {}, " +
						"but actual allocation id {} differs. Ignoring the request.", slotId, allocationId, slot.getAllocationId());
				}
			} else {
				LOG.debug("Slot {} has not been allocated.", allocationId);
			}
		} else {
			LOG.debug("Trying to free a slot {} which has not been registered. Ignoring this message.", slotId);
		}
	}

	@Override
	public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
		if (!this.failUnfulfillableRequest && failUnfulfillableRequest) {
			// fail unfulfillable pending requests
			Iterator<Entry<AllocationID, PendingSlotRequest>> slotRequestIterator = pendingSlotRequests.entrySet().iterator();
			while (slotRequestIterator.hasNext()) {
				PendingSlotRequest pendingSlotRequest = slotRequestIterator.next().getValue();
				if (pendingSlotRequest.getAssignedPendingTaskManagerSlot() != null) {
					continue;
				}
				if (isNotFulfillableByRegisteredTaskManagers(pendingSlotRequest.getResourceProfile())) {
					slotRequestIterator.remove();
					resourceActions.notifyAllocationFailure(
						pendingSlotRequest.getJobId(),
						pendingSlotRequest.getAllocationId(),
						new UnfulfillableSlotRequestException(pendingSlotRequest.getAllocationId(), pendingSlotRequest.getResourceProfile())
					);
				}
			}
		}
		this.failUnfulfillableRequest = failUnfulfillableRequest;
	}

	// ---------------------------------------------------------------------------------------------
	// Behaviour methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Finds a matching slot request for a given resource profile. If there is no such request,
	 * the method returns null.
	 *
	 * <p>Note: If you want to change the behaviour of the slot manager wrt slot allocation and
	 * request fulfillment, then you should override this method.
	 *
	 * @param availableResource defining the available resources of task executor
	 * @return A matching slot request which can be deployed in a slot with the given resource
	 * profile. Null if there is no such slot request pending.
	 */
	private Optional<PendingSlotRequest> findMatchingRequest(TaskManagerAvailableResource availableResource) {
		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			if (!pendingSlotRequest.isAssigned() &&
				availableResource.canBePotentiallyAllocated(pendingSlotRequest.getResourceProfile())) {

				return Optional.of(pendingSlotRequest);
			}
		}
		return Optional.empty();
	}

	private Optional<TaskManagerRegistration> findMatchingTaskManagerAvailableResource(
			PendingSlotRequest pendingSlotRequest) {
		for (TaskManagerRegistration registration : taskManagerRegistrations.values()) {
			if (registration.getAvailableResource().canBeAssignedPendingSlotRequest(pendingSlotRequest)) {
				return Optional.of(registration);
			}
		}
		return Optional.empty();
	}

	// ---------------------------------------------------------------------------------------------
	// Internal slot operations
	// ---------------------------------------------------------------------------------------------

	private Optional<TaskManagerPendingResource> findExactlyMatchingPendingResource(ResourceProfile resourceProfile) {
		TaskManagerPendingResource foundPendingResource = null;
		for (TaskManagerPendingResource pendingResource : pendingResources.values()) {
			if (pendingResource.inTotalSameAs(resourceProfile)) {
				foundPendingResource = pendingResource;
				break;
			}
		}
		return Optional.ofNullable(foundPendingResource);
	}

	private void internalRequestSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
		Optional<TaskManagerRegistration> taskManagerAvailableResource =
			findMatchingTaskManagerAvailableResource(pendingSlotRequest);

		if (taskManagerAvailableResource.isPresent()) {
			allocateSlot(taskManagerAvailableResource.get(), pendingSlotRequest);
		} else {
			Optional<TaskManagerPendingResource> pendingResource =
				findFreeMatchingTaskManagerPendingResource(pendingSlotRequest);

			if (!pendingResource.isPresent()) {
				pendingResource = allocateResource(pendingSlotRequest);
			}

			if (!pendingResource.isPresent()) {
				// request can not be fulfilled by any free slot or pending slot that can be allocated,
				// check whether it can be fulfilled by allocated slots
				if (failUnfulfillableRequest &&
					isNotFulfillableByRegisteredTaskManagers(pendingSlotRequest.getResourceProfile())) {

					throw new UnfulfillableSlotRequestException(
						pendingSlotRequest.getAllocationId(),
						pendingSlotRequest.getResourceProfile());
				}
			}
		}
	}

	private Optional<TaskManagerPendingResource> findFreeMatchingTaskManagerPendingResource(
			PendingSlotRequest pendingSlotRequest) {
		for (TaskManagerPendingResource pendingResource : pendingResources.values()) {
			if (pendingResource.canBeAssignedPendingSlotRequest(pendingSlotRequest)) {
				return Optional.of(pendingResource);
			}
		}
		return Optional.empty();
	}

	private boolean isNotFulfillableByRegisteredTaskManagers(ResourceProfile resourceProfile) {
		for (TaskManagerRegistration registration : taskManagerRegistrations.values()) {
			if (registration.getAvailableResource().canBePotentiallyAllocated(resourceProfile)) {
				return false;
			}
		}
		return true;
	}

	private Optional<TaskManagerPendingResource> allocateResource(
			PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
		Collection<ResourceProfile> slotsBeingAllocated =
			resourceActions.allocateResource(pendingSlotRequest.getResourceProfile());
		final Optional<TaskManagerPendingResource> pendingResourceOptional = slotsBeingAllocated
			.stream()
			.reduce(ResourceProfile::merge)
			.map(r -> new TaskManagerPendingResource(r, slotsBeingAllocated.iterator().next()));
		pendingResourceOptional.ifPresent(pendingResource -> {
			pendingResources.put(pendingResource.getId(), pendingResource);
			pendingResource.assignPendingSlotRequest(pendingSlotRequest);
		});
		return pendingResourceOptional;
	}

	private void allocateSlot(
			TaskManagerRegistration taskManagerRegistration,
			PendingSlotRequest pendingSlotRequest) {

		TaskExecutorConnection taskExecutorConnection = taskManagerRegistration.getTaskManagerConnection();
		TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();
		InstanceID instanceID = taskExecutorConnection.getInstanceID();
		CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
		AllocationID allocationId = pendingSlotRequest.getAllocationId();

		pendingSlotRequest.setRequestFuture(completableFuture);
		taskManagerRegistration.getAvailableResource().assignPendingSlotRequest(pendingSlotRequest);

		taskManagerRegistration.markUsed();

		// RPC call to the task manager
		CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot(
			SlotID.generateDynamicSlotID(taskManagerRegistration.getResourceID()),
			pendingSlotRequest.getJobId(),
			pendingSlotRequest.getAllocationId(),
			pendingSlotRequest.getResourceProfile(),
			pendingSlotRequest.getTargetAddress(),
			resourceManagerId,
			taskManagerRequestTimeout);

		requestFuture.whenComplete(
			(Acknowledge acknowledge, Throwable throwable) -> {
				if (acknowledge != null) {
					completableFuture.complete(acknowledge);
				} else {
					completableFuture.completeExceptionally(throwable);
				}
			});

		completableFuture.whenCompleteAsync(
			(Acknowledge acknowledge, Throwable throwable) -> {
				try {
					//noinspection VariableNotUsedInsideIf
					if (acknowledge != null) {
						taskManagerRegistration.getAvailableResource().pendingRequestAllocated(pendingSlotRequest);
					} else {
						taskManagerRegistration.getAvailableResource().rejectPendingSlotRequest(pendingSlotRequest, throwable);
						handleFreeResource(taskManagerRegistration);
						if (throwable instanceof CancellationException) {
							LOG.debug("Slot allocation request {} has been cancelled.", allocationId, throwable);
						} else {
							handleFailedSlotRequest(instanceID, pendingSlotRequest, throwable);
						}
					}
				} catch (RuntimeException e) {
					LOG.error("Error while completing the slot allocation.", e);
				}
			},
			mainThreadExecutor);
	}

	private void handleFreeResource(TaskManagerRegistration registration) {
		findMatchingRequest(registration.getAvailableResource()).ifPresent(pendingSlotRequest ->
			allocateSlot(registration, pendingSlotRequest)
		);
	}

	/**
	 * Handles a failed slot request. The slot manager tries to find a new slot fulfilling
	 * the resource requirements for the failed slot request.
	 *
	 * @param instanceID identifying the task executor for which the slot request failed
	 * @param pendingSlotRequest the failed slot request
	 * @param cause of the failure
	 */
	private void handleFailedSlotRequest(InstanceID instanceID, PendingSlotRequest pendingSlotRequest, Throwable cause) {
		LOG.debug("Slot request with allocation id {} failed for task manager {}.", pendingSlotRequest.getAllocationId(), instanceID, cause);
		pendingSlotRequest.setRequestFuture(null);
		try {
			internalRequestSlot(pendingSlotRequest);
		} catch (ResourceManagerException e) {
			pendingSlotRequests.remove(pendingSlotRequest.getAllocationId());
			resourceActions.notifyAllocationFailure(
				pendingSlotRequest.getJobId(),
				pendingSlotRequest.getAllocationId(),
				e);
		}
	}

	/**
	 * Cancels the given slot request.
	 *
	 * @param pendingSlotRequest to cancel
	 */
	private void cancelPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
		CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

		// TODO:

		if (null != request) {
			request.cancel(false);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal timeout methods
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	void checkTaskManagerTimeouts() {
		if (!taskManagerRegistrations.isEmpty()) {
			long currentTime = System.currentTimeMillis();

			Collection<TaskManagerRegistration> timedOutTaskManagers = new ArrayList<>();

			// first retrieve the timed out TaskManagers
			for (TaskManagerRegistration taskManagerRegistration : taskManagerRegistrations.values()) {
				if (currentTime - taskManagerRegistration.getIdleSince() >= taskManagerTimeout.toMilliseconds()) {
					// we collect the instance ids first in order to avoid concurrent modifications by the
					// ResourceActions.releaseResource call
					timedOutTaskManagers.add(taskManagerRegistration);
				}
			}

			// second we trigger the release resource callback which can decide upon the resource release
			for (TaskManagerRegistration taskManagerRegistration : timedOutTaskManagers) {
				if (waitResultConsumedBeforeRelease) {
					releaseTaskExecutorIfPossible(taskManagerRegistration);
				} else {
					releaseTaskExecutor(taskManagerRegistration.getInstanceId());
				}
			}
		}
	}

	private void releaseTaskExecutorIfPossible(TaskManagerRegistration taskManagerRegistration) {
		long idleSince = taskManagerRegistration.getIdleSince();
		taskManagerRegistration
			.getTaskManagerConnection()
			.getTaskExecutorGateway()
			.canBeReleased()
			.thenAcceptAsync(
				canBeReleased -> {
					InstanceID timedOutTaskManagerId = taskManagerRegistration.getInstanceId();
					boolean stillIdle = idleSince == taskManagerRegistration.getIdleSince();
					if (stillIdle && canBeReleased) {
						releaseTaskExecutor(timedOutTaskManagerId);
					}
				},
				mainThreadExecutor);
	}

	private void releaseTaskExecutor(InstanceID timedOutTaskManagerId) {
		final FlinkException cause = new FlinkException("TaskExecutor exceeded the idle timeout.");
		LOG.debug("Release TaskExecutor {} because it exceeded the idle timeout.", timedOutTaskManagerId);
		resourceActions.releaseResource(timedOutTaskManagerId, cause);
	}

	private void checkSlotRequestTimeouts() {
		if (!pendingSlotRequests.isEmpty()) {
			long currentTime = System.currentTimeMillis();

			Iterator<Entry<AllocationID, PendingSlotRequest>> slotRequestIterator = pendingSlotRequests.entrySet().iterator();

			while (slotRequestIterator.hasNext()) {
				PendingSlotRequest slotRequest = slotRequestIterator.next().getValue();

				if (currentTime - slotRequest.getCreationTimestamp() >= slotRequestTimeout.toMilliseconds()) {
					slotRequestIterator.remove();

					if (slotRequest.isAssigned()) {
						cancelPendingSlotRequest(slotRequest);
					}

					resourceActions.notifyAllocationFailure(
						slotRequest.getJobId(),
						slotRequest.getAllocationId(),
						new TimeoutException("The allocation could not be fulfilled in time."));
				}
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal utility methods
	// ---------------------------------------------------------------------------------------------

	private void internalUnregisterTaskManager(TaskManagerRegistration taskManagerRegistration, Exception cause) {
		Preconditions.checkNotNull(taskManagerRegistration);

		// TODO: implement
	}

	private boolean checkDuplicateRequest(AllocationID allocationId) {
		return pendingSlotRequests.containsKey(allocationId); // TODO: check || allocatedSlots.containsKey(allocationId);
	}

	private void checkInit() {
		Preconditions.checkState(started, "The slot manager has not been started.");
	}

	// ---------------------------------------------------------------------------------------------
	// Testing methods
	// ---------------------------------------------------------------------------------------------

	@Override
	@VisibleForTesting
	public void unregisterTaskManagersAndReleaseResources() {
		Iterator<Entry<InstanceID, TaskManagerRegistration>> taskManagerRegistrationIterator =
			taskManagerRegistrations.entrySet().iterator();

		while (taskManagerRegistrationIterator.hasNext()) {
			TaskManagerRegistration taskManagerRegistration =
				taskManagerRegistrationIterator.next().getValue();

			taskManagerRegistrationIterator.remove();

			final FlinkException cause = new FlinkException("Triggering of SlotManager#unregisterTaskManagersAndReleaseResources.");
			internalUnregisterTaskManager(taskManagerRegistration, cause);
			resourceActions.releaseResource(taskManagerRegistration.getInstanceId(), cause);
		}
	}
}
