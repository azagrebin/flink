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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;

public class BatchFineGrainedRecoveryITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().set

		DataSet<Integer> input = env.createInput(...);

		input
			.flatMap(new RichFlatMapFunction<Integer, Integer>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void flatMap(Integer value, Collector<Integer> out) {

				}
			})
			.groupBy(i -> i % 10)
			.reduceGroup(new RichGroupReduceFunction<Integer, Integer>() {
				@Override
				public void reduce(Iterable<Integer> values, Collector<Integer> out) {

				}
			})
			.output(...);

		env.execute();
	}

	private abstract static class RichTrackedFunction extends AbstractRichFunction {
		private final Tracker tracker;
		private final RegionID regionID;
		private TaskInfo taskInfo;
		private boolean started;

		protected RichTrackedFunction(Tracker tracker, RegionID regionID) {
			this.tracker = tracker;
			this.regionID = regionID;
		}

		protected void doTracking(Element<?> element) {
			if (taskInfo == null) {
				taskInfo = new TaskInfo(getRuntimeContext().getTaskName(), new RegionInfo(regionID, getRuntimeContext().getIndexOfThisSubtask()));
			}

			if (element.type == ElementType.FIRST) {
				started = true;
				tracker.trackEvent();
			}
		}
	}

	private enum TaskState {
		CREATED,
		STARTED,
		LAST
	}

	private static class Element<T> {
		private final ElementType type;
		private final T data;
	}

	private enum ElementType {
		FIRST,
		MIDDLE,
		LAST
	}

	private interface Tracker extends Serializable {
		void trackEvent(Event event);
	}

	private static class TrackerImpl {
		private static final long serialVersionUID = 1L;

		private static final Queue<Event> eventQueue;

		@Override
		public void trackEvent(Event event) {
			eventQueue.add(event);
		}
	}

	private static class RegionGraph {
		private final Collection<Region> inputRegions;
		private final Map<RegionInfo, Region> allRegions;
		private final Map<TaskID, TaskInfo> allTasks;
	}

	private static class Region {
		private final RegionInfo regionInfo;
		private final Collection<Region> children;
		private final Collection<TaskID> tasks;

		private Region(RegionInfo regionInfo) {
			this.regionInfo = regionInfo;
			this.children = new ArrayList<>();
			this.tasks = new ArrayList<>();
		}


	}

	private static class RegionID extends AbstractID {
		private static final long serialVersionUID = 1L;
	}

	private static class TaskID extends AbstractID {
		private static final long serialVersionUID = 1L;
	}

	private static class TaskInfo {
		private final String name;
		private final TaskID id;
		private final RegionInfo regionInfo;

		private TaskInfo(String name, RegionInfo regionInfo) {
			this(name, new TaskID(), regionInfo);
		}

		private TaskInfo(String name, TaskID id, RegionInfo regionInfo) {
			this.name = name;
			this.id = id;
			this.regionInfo = regionInfo;
		}

		public String getName() {
			return name;
		}

		public TaskID getId() {
			return id;
		}

		public RegionInfo getRegionInfo() {
			return regionInfo;
		}
	}

	private static class RegionInfo {
		private final RegionID id;
		private final int parallelIndex;

		private RegionInfo(RegionID id, int parallelIndex) {
			this.id = id;
			this.parallelIndex = parallelIndex;
		}

		public RegionID getId() {
			return id;
		}

		public int getParallelIndex() {
			return parallelIndex;
		}
	}

	private abstract static class Event {
		private final TaskInfo taskInfo;

		protected Event(TaskInfo taskInfo) {
			this.taskInfo = taskInfo;
		}

		public TaskInfo getTaskInfo() {
			return taskInfo;
		}
	}
}
