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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.ParallelIteratorInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SplittableIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.StreamSupport;

public class BatchFineGrainedRecoveryITCase extends JavaProgramTestBase {
	private static final int MAP_PARALLELISM = 3;
	private static final int REDUCE_PARALLELISM = 2;

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(1)));

		DataSet<Integer> input = env.createInput(Generator.generate(30, 10)).setParallelism(MAP_PARALLELISM).map(t -> t.f1);

		Tracker tracker = new TrackerImpl();
		FailureStrategy failureStrategy = new FailureStrategyImpl(1, 1000);

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
			.setParallelism(MAP_PARALLELISM)
			.groupBy(i -> i % 10)
			.reduceGroup(new RichTrackedGroupReduceFunction<Integer, Integer>(tracker, failureStrategy) {
				private static final long serialVersionUID = 1L;

				@Override
				void doReduce(Iterable<Integer> values, Collector<Integer> out) {
					out.collect(StreamSupport.stream(values.spliterator(), false).mapToInt(i -> i).sum());
				}
			})
			.setParallelism(REDUCE_PARALLELISM)
			.output(new RichOutputFormatStub());

		env.execute();
	}

	private static class RichOutputFormatStub extends RichOutputFormat<Integer> {
		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {

		}

		@Override
		public void writeRecord(Integer record) throws IOException {

		}

		@Override
		public void close() throws IOException {

		}
	}

	private static class TrackedTask implements Serializable {
		private static final long serialVersionUID = 1L;

		private final RegionID regionID;
		private final Tracker tracker;
		private final FailureStrategy failureStrategy;
		private TaskInfo taskInfo;

		private TrackedTask(Tracker tracker, FailureStrategy failureStrategy) {
			this.regionID = new RegionID();
			this.tracker = tracker;
			this.failureStrategy = failureStrategy;
		}

		private void startTracking(RuntimeContext context) {
			startTracking(context.getTaskName(), context.getIndexOfThisSubtask());
		}

		private void startTracking(String name, int taskNumber) {
			taskInfo = new TaskInfo(name, new RegionInfo(regionID, taskNumber));
			tracker.trackEvent(new StartEvent(taskInfo));
		}

		private void stopTracking() {
			tracker.trackEvent(new FinishEvent(taskInfo));
		}

		private void failOrNot() {
			try {
				failureStrategy.failOrNot(taskInfo);
			} catch (Throwable t) {
				tracker.trackEvent(new FailureEvent(taskInfo));
				throw t;
			}
		}
	}

	private static class TrackedParallelIteratorInputFormat<T> extends ParallelIteratorInputFormat<T> {
		private final TrackedTask trackedTask;

		private TrackedParallelIteratorInputFormat(SplittableIterator<T> iterator, Tracker tracker, FailureStrategy failureStrategy) {
			super(iterator);
			this.trackedTask = new TrackedTask(tracker, failureStrategy);
		}

		@Override
		public void open(GenericInputSplit split) throws IOException {
			super.open(split);
			trackedTask.startTracking(getRuntimeContext());
		}

		@Override
		public void close() throws IOException {
			trackedTask.stopTracking();
			super.close();
		}
	}

	private static class RandomSplittableIterator extends SplittableIterator<Integer> {

		@Override
		public Iterator<Integer>[] split(int numPartitions) {
			return new Iterator[0];
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return 0;
		}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public Integer next() {
			return null;
		}
	}

	private abstract static class RichTrackedFunction extends AbstractRichFunction {
		private static final long serialVersionUID = 1L;

		final TrackedTask trackedTask;

		private RichTrackedFunction(Tracker tracker, FailureStrategy failureStrategy) {
			this.trackedTask = new TrackedTask(tracker, failureStrategy);
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			trackedTask.startTracking(getRuntimeContext());
		}

		@Override
		public void close() throws Exception {
			trackedTask.stopTracking();
			super.close();
		}
	}

	private abstract static class RichTrackedFlatMapFunction<T, O> extends RichTrackedFunction implements FlatMapFunction<T, O> {
		private static final long serialVersionUID = 1L;

		private RichTrackedFlatMapFunction(Tracker tracker, FailureStrategy failureStrategy) {
			super(tracker, failureStrategy);
		}

		@Override
		public void flatMap(T value, Collector<O> out) {
			trackedTask.failOrNot();
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
			trackedTask.failOrNot();
			doReduce(values, out);
		}

		abstract void doReduce(Iterable<T> values, Collector<O> out);
	}

	private static class FailureStrategyImpl implements FailureStrategy {
		private static final Random rnd = new Random();

		private final int probFraction;
		private final int probBase;
		private int callCount;

		private FailureStrategyImpl(int probFraction, int probBase) {
			this.probFraction = probFraction;
			this.probBase = probBase;
		}

		@Override
		public void failOrNot(TaskInfo taskInfo) {
			callCount++;
			int prob = rnd.nextInt(probBase) + 1;
			if (prob <= probFraction) {
				throw new FineGrainedRecoveryTestFailure(taskInfo);
			}
		}
	}

	private interface FailureStrategy extends Serializable {
		void failOrNot(TaskInfo taskInfo);
	}

	private static class FineGrainedRecoveryTestFailure extends FlinkRuntimeException {
		private static final long serialVersionUID = 1L;

		private FineGrainedRecoveryTestFailure(TaskInfo taskInfo) {
			super(String.format("BOOM!!! Generate failure in %s", taskInfo));
		}
	}

	@FunctionalInterface
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

		@Override
		public String toString() {
			return "{" +
				"rid=" + id +
				", index=" + parallelIndex +
				'}';
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

		@Override
		public String toString() {
			return "TI{" +
				"'" + name + '\'' +
				", id=" + id +
				"(" + regionInfo +
				")}";
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

		@Override
		public String toString() {
			return "{" +
				"" + taskInfo +
				'}';
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

	public static class Generator implements InputFormat<Tuple2<String, Integer>, GenericInputSplit> {

		// total number of records
		private final long numRecords;
		// total number of keys
		private final long numKeys;

		// records emitted per partition
		private long recordsPerPartition;
		// number of keys per partition
		private long keysPerPartition;

		// number of currently emitted records
		private long recordCnt;

		// id of current partition
		private int partitionId;

		private final boolean infinite;

		public static Generator generate(long numKeys, int recordsPerKey) {
			return new Generator(numKeys, recordsPerKey, false);
		}

		public static Generator generateInfinitely(long numKeys) {
			return new Generator(numKeys, 0, true);
		}

		private Generator(long numKeys, int recordsPerKey, boolean infinite) {
			this.numKeys = numKeys;
			if (infinite) {
				this.numRecords = Long.MAX_VALUE;
			} else {
				this.numRecords = numKeys * recordsPerKey;
			}
			this.infinite = infinite;
		}

		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
			return null;
		}

		@Override
		public GenericInputSplit[] createInputSplits(int minNumSplits) {

			GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
			for (int i = 0; i < minNumSplits; i++) {
				splits[i] = new GenericInputSplit(i, minNumSplits);
			}
			return splits;
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] inputSplits) {
			return new DefaultInputSplitAssigner(inputSplits);
		}

		@Override
		public void open(GenericInputSplit split) throws IOException {
			this.partitionId = split.getSplitNumber();
			// total number of partitions
			int numPartitions = split.getTotalNumberOfSplits();

			// ensure even distribution of records and keys
			Preconditions.checkArgument(
				numRecords % numPartitions == 0,
				"Records cannot be evenly distributed among partitions");
			Preconditions.checkArgument(
				numKeys % numPartitions == 0,
				"Keys cannot be evenly distributed among partitions");

			this.recordsPerPartition = numRecords / numPartitions;
			this.keysPerPartition = numKeys / numPartitions;

			this.recordCnt = 0;
		}

		@Override
		public boolean reachedEnd() {
			return !infinite && this.recordCnt >= this.recordsPerPartition;
		}

		@Override
		public Tuple2<String, Integer> nextRecord(Tuple2<String, Integer> reuse) throws IOException {

			// build key from partition id and count per partition
			String key = String.format(
				"%d-%d",
				this.partitionId,
				this.recordCnt % this.keysPerPartition);

			// 128 values to filter on
			int filterVal = (int) this.recordCnt % 128;

			this.recordCnt++;

			reuse.f0 = key;
			reuse.f1 = filterVal;
			return reuse;
		}

		@Override
		public void close() {
		}
	}
}
