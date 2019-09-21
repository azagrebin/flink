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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Builder for {@link TaskSlotTable}. */
public class TaskSlotTableBuilder {
	private static final TimerService<AllocationID> DEFAULT_TIMER_SERVICE =
		new TimerService<>(TestingUtils.defaultExecutor(), 10000L);

	private List<TaskSlot> taskSlots;
	private TimerService<AllocationID> timerService = DEFAULT_TIMER_SERVICE;

	private TaskSlotTableBuilder setTaskSlots(List<TaskSlot> taskSlots) {
		this.taskSlots = new ArrayList<>(taskSlots);
		return this;
	}

	public TaskSlotTableBuilder setTimerService(TimerService<AllocationID> timerService) {
		this.timerService = timerService;
		return this;
	}

	public TaskSlotTableBuilder withTimerServiceTimeout(Time timeout) {
		this.timerService = new TimerService<>(TestingUtils.defaultExecutor(), timeout.toMilliseconds());
		return this;
	}

	public TaskSlotTable build() {
		return new TaskSlotTable(taskSlots, timerService);
	}

	public static TaskSlotTableBuilder newBuilder() {
		return newBuilderWithDefaultSlots(1);
	}

	public static TaskSlotTableBuilder newBuilderWithDefaultSlots(int numberOfDefaultSlots) {
		return new TaskSlotTableBuilder().setTaskSlots(createDefaultSlots(numberOfDefaultSlots));
	}

	public static List<TaskSlot> createDefaultSlots(int numberOfDefaultSlots) {
		return TaskManagerServices.createTaskSlotsFromResources(IntStream
			.range(0, numberOfDefaultSlots)
			.mapToObj(i -> ResourceProfile.UNKNOWN)
			.collect(Collectors.toList()));
	}
}
