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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A report about the current status of all slots of the TaskExecutor, describing
 * which slots are available and allocated, and what jobs (JobManagers) the allocated slots
 * have been allocated to.
 */
public class SlotReport implements Serializable, Iterable<SlotStatus> {

	private static final long serialVersionUID = -3150175198722481689L;

	/** The slots status of the TaskManager. */
	private final Collection<SlotStatus> slotsStatus;

	private final ResourceProfile availableResource;

	@VisibleForTesting
	public SlotReport() {
		this(Collections.<SlotStatus>emptyList());
	}

	@VisibleForTesting
	public SlotReport(SlotStatus slotStatus) {
		this(Collections.singletonList(slotStatus));
	}

	public SlotReport(final Collection<SlotStatus> slotsStatus) {
		this(slotsStatus,
			slotsStatus.stream()
				.filter(status -> status.getAllocationID() == null)
				.map(SlotStatus::getResourceProfile)
				.reduce((acc, rp) -> acc.merge(rp))
				.orElse(ResourceProfile.ZERO));
	}

	public SlotReport(final Collection<SlotStatus> slotsStatus, final ResourceProfile availableResource) {
		this.slotsStatus = checkNotNull(slotsStatus);
		this.availableResource = checkNotNull(availableResource);
	}

	@Override
	public Iterator<SlotStatus> iterator() {
		return slotsStatus.iterator();
	}

	public ResourceProfile getAvailableResource() {
		return availableResource;
	}

	@Override
	public String toString() {
		return "SlotReport{" +
			"slotsStatus=" + slotsStatus +
			", availableResource" + availableResource +
			'}';
	}
}
