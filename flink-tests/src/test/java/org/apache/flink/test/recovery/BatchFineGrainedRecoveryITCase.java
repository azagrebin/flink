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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.StreamSupport;

public class BatchFineGrainedRecoveryITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> input = env.createInput(...);

		Tracker tracker = new TrackerImpl();
		FailureStrategy failureStrategy = new FailureStrategyImpl(50, 100);

		input
			.flatMap(new RichTrackedFlatMapFunction<Integer, Integer>(tracker, failureStrategy) {
				private static final long serialVersionUID = 1L;
				private final Random rnd = new Random();

				@Override
				void doFlatMap(Integer value, Collector<Integer> out) {
					out.collect(value + rnd.nextInt(100));
					out.collect(value + rnd.nextInt(100));
					out.collect(value + rnd.nextInt(100));
				}
			})
			.setParallelism(5)
			.groupBy(i -> i % 10)
			.reduceGroup(new RichTrackedGroupReduceFunction<Integer, Integer>(tracker, failureStrategy) {
				private static final long serialVersionUID = 1L;

				@Override
				void doReduce(Iterable<Integer> values, Collector<Integer> out) {
					out.collect(StreamSupport.stream(values.spliterator(), false).mapToInt(i -> i).sum());
				}
			})
			.setParallelism(3)
			.output(...);

		env.execute();
	}

	private abstract static class RichTrackedFunction extends AbstractRichFunction {
		private static final long serialVersionUID = 1L;

		private final RegionID regionID;
		private final Tracker tracker;
		private final FailureStrategy failureStrategy;
		private TaskInfo taskInfo;

		RichTrackedFunction(Tracker tracker, FailureStrategy failureStrategy) {
			this.regionID = new RegionID();
			this.tracker = tracker;
			this.failureStrategy = failureStrategy;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			taskInfo = new TaskInfo(getRuntimeContext().getTaskName(), new RegionInfo(regionID, getRuntimeContext().getIndexOfThisSubtask()));
			tracker.trackEvent(new StartEvent(taskInfo));
		}

		@Override
		public void close() throws Exception {
			tracker.trackEvent(new FinishEvent(taskInfo));
			super.close();
		}

		protected void failOrNot() {
			failureStrategy.failOrNot(taskInfo, tracker);
		}
	}

	private abstract static class RichTrackedFlatMapFunction<T, O> extends RichTrackedFunction implements FlatMapFunction<T, O> {
		private static final long serialVersionUID = 1L;

		private RichTrackedFlatMapFunction(Tracker tracker, FailureStrategy failureStrategy) {
			super(tracker, failureStrategy);
		}

		@Override
		public void flatMap(T value, Collector<O> out) {
			failOrNot();
			doFlatMap(value, out);
		}

		abstract void doFlatMap(T value, Collector<O> out);
	}

	private abstract static class RichTrackedGroupReduceFunction<T, O> extends RichTrackedFunction implements GroupReduceFunction<T, O> {
		private static final long serialVersionUID = 1L;

		private RichTrackedGroupReduceFunction(Tracker tracker, FailureStrategy failureStrategy) {
			super(tracker, failureStrategy);
		}

		@Override
		public void reduce(Iterable<T> values, Collector<O> out) throws Exception {
			failOrNot();
			doReduce(values, out);
		}

		abstract void doReduce(Iterable<T> values, Collector<O> out);
	}

	private static class FailureStrategyImpl implements FailureStrategy {
		private static final Random rnd = new Random();

		private final int failureCount;
		private int callCount;

		private FailureStrategyImpl(int failureCountMin, int failureCountMax) {
			this.failureCount = (rnd.nextInt() % failureCountMax) + failureCountMin;
		}

		@Override
		public void failOrNot(TaskInfo taskInfo, Tracker tracker) {
			tracker.trackEvent(new FailureEvent(taskInfo));
			if (callCount % 100 == 0) {
				throw new FineGrainedRecoveryTestFailure(taskInfo);
			}
		}
	}

	private interface FailureStrategy {
		void failOrNot(TaskInfo taskInfo, Tracker tracker);
	}

	private static class FineGrainedRecoveryTestFailure extends FlinkRuntimeException {
		private FineGrainedRecoveryTestFailure(TaskInfo taskInfo) {
			super(String.format("BOOM!!! Generate failure in %s", taskInfo));
		}
	}

	private interface Tracker extends Serializable {
		void trackEvent(Event event);
	}

	private static class TrackerImpl implements Tracker {
		private static final long serialVersionUID = 1L;

		private static final Queue<Event> eventQueue = new ConcurrentLinkedQueue<>();

		@Override
		public void trackEvent(Event event) {
			eventQueue.add(event);
		}
	}

//	private static class RegionGraph {
//		private final Collection<Region> inputRegions;
//		private final Map<RegionInfo, Region> allRegions;
//		private final Map<TaskID, TaskInfo> allTasks;
//	}

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

	private enum TaskState {
		CREATED,
		STARTED,
		LAST
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

	private static class StartEvent extends Event {
		private StartEvent(TaskInfo taskInfo) {
			super(taskInfo);
		}
	}

	private static class FailureEvent extends Event {
		private FailureEvent(TaskInfo taskInfo) {
			super(taskInfo);
		}
	}

	private static class FinishEvent extends Event {
		private FinishEvent(TaskInfo taskInfo) {
			super(taskInfo);
		}
	}
}
