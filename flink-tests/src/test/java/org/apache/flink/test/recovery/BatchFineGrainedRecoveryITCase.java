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

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.Utils.CollectHelper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview.JobIdWithStatus;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration.Builder;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.JobIdsWithStatusesOverviewHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo;
import org.apache.flink.runtime.rest.messages.JobVertexDetailsInfo.VertexTaskDetail;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.flink.configuration.JobManagerOptions.FORCE_PARTITION_RELEASE_ON_CONSUMPTION;
import static org.apache.flink.runtime.executiongraph.failover.FailoverStrategyLoader.PIPELINED_REGION_RESTART_STRATEGY_NAME;
import static org.apache.flink.util.ExceptionUtils.getOrRethrow;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * IT case for fine-grained recovery of batch jobs.
 *
 * <p>The test activates the region fail-over strategy to restart only failed producers.
 * The test job is a sequence of non-parallel mappers. Each mapper writes to a blocking partition which means
 * the next mapper starts when the previous is done. The mappers are not chained into one task which makes them
 * separate fail-over regions.
 *
 * <p>The test verifies that fine-grained recovery works by randomly incuding failures in any of the mappers.
 * Since all mappers are connected via blocking partitions, which should be re-used on failure, and the consumer
 * of the mapper wasn't deployed yet, as the consumed partition was not fully produced yet, only the failed mapper
 * should actually restart.
 */
public class BatchFineGrainedRecoveryITCase extends TestLogger {
	private static final int EMITTED_RECORD_NUMBER = 1000;
	private static final int MAX_FAILURE_NUMBER = 10;
	private static final int MAP_NUMBER = 3;
	private static final String TASK_NAME_PREFIX = "Test partition mapper ";
	private static final List<Long> EXPECTED_JOB_OUTPUT =
		LongStream.range(MAP_NUMBER, EMITTED_RECORD_NUMBER + MAP_NUMBER).boxed().collect(Collectors.toList());

	private static TestingMiniCluster miniCluster;
	private static MiniClusterClient client;

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setBoolean(FORCE_PARTITION_RELEASE_ON_CONSUMPTION, false);
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, PIPELINED_REGION_RESTART_STRATEGY_NAME);

		miniCluster = new TestingMiniCluster(
			new Builder()
				.setNumTaskManagers(1)
				.setNumSlotsPerTaskManager(1)
				.setConfiguration(configuration)
				.setRpcServiceSharing(RpcServiceSharing.DEDICATED)
				.build(),
			null);

		miniCluster.start();

		client = new MiniClusterClient(miniCluster);
	}

	@After
	public void teardown() throws Exception {
		if (miniCluster != null) {
			miniCluster.close();
		}
		if (client != null) {
			client.shutdown();
		}
	}

	@Test
	public void testProgram() throws Exception {
		ExecutionEnvironment env = createExecutionEnvironment();

		StaticFailureCounter.reset();
		StaticMapFailureTracker.reset();

		CoinToss coin = new CoinToss(1, EMITTED_RECORD_NUMBER);
		FailureStrategy failureStrategy = new JoinedFailureStrategy(
			//new RandomExceptionFailureStrategy(coin),
			new RandomTaskExecutorFailureStrategy(coin));

		DataSet<Long> input = env.generateSequence(0, EMITTED_RECORD_NUMBER - 1);
		for (int i = 0; i < MAP_NUMBER; i++) {
			input = input
				.mapPartition(new TestPartitionMapper(StaticMapFailureTracker.addNewMap(), i == 0 ? t -> {} : failureStrategy))
				.name(TASK_NAME_PREFIX + i);
		}
		Tuple2<JobExecutionResult, List<Long>> resAndOut = collect(env, input);
		JobExecutionResult res = resAndOut.f0;

		assertThat(resAndOut.f1, is(EXPECTED_JOB_OUTPUT));

		//JobGraph jobGraph = new JobGraphGenerator().compileJobGraph(new Optimizer(new DataStatistics(), new Configuration()).compile(env.createProgramPlan()));

		JobDetailsInfo jobInfo = client.getJobDetails(res.getJobID()).get();

		StaticMapFailureTracker.verify();
	}

	private static <T> Tuple2<JobExecutionResult, List<T>> collect(ExecutionEnvironment env, DataSet<T> input) throws Exception {
		final String id = new AbstractID().toString();
		final TypeSerializer<T> serializer = input.getType().createSerializer(env.getConfig());
		input.output(new CollectHelper<>(id, serializer)).name("collect()");
		JobExecutionResult res = env.execute();
		List<T> accResult = SerializedListAccumulator.deserializeList(res.getAccumulatorResult(id), serializer);
		return Tuple2.of(res, accResult);
	}

	private static ExecutionEnvironment createExecutionEnvironment() {
		@SuppressWarnings("StaticVariableUsedBeforeInitialization")
		ExecutionEnvironment env = new TestEnvironment(miniCluster, 1, true);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(MAX_FAILURE_NUMBER, Time.milliseconds(10)));
		env.getConfig().setExecutionMode(ExecutionMode.BATCH_FORCED); // forces all partitions to be blocking
		return env;
	}

	private enum StaticMapFailureTracker {
		;

		private static final List<AtomicInteger> mapRestarts = new ArrayList<>(10);
		private static final List<AtomicInteger> expectedMapRestarts = new ArrayList<>(10);

		private static void reset() {
			mapRestarts.clear();
			expectedMapRestarts.clear();
		}

		private static int addNewMap() {
			mapRestarts.add(new AtomicInteger(0));
			expectedMapRestarts.add(new AtomicInteger(1));
			return mapRestarts.size() - 1;
		}

		private static void mapRestart(int index) {
			mapRestarts.get(index).incrementAndGet();
		}

		private static void mapFailure(int index) {
			expectedMapRestarts.get(index).incrementAndGet();
		}

		private static void mapFailure(String name) {
			if (name.contains(TASK_NAME_PREFIX)) {
				int index = TaskTrackingIndexConverter.taskNameToId(name);
				IntStream.range(0, index + 1).forEach(i -> expectedMapRestarts.get(i).incrementAndGet());
				IntStream.range(0, index + 1).forEach(i -> expectedMapRestarts.get(i).incrementAndGet());
			}
		}

		private static void verify() {
			assertThat(collect(mapRestarts), is(collect(expectedMapRestarts)));
		}

		private static int[] collect(Collection<AtomicInteger> list) {
			return list.stream().mapToInt(AtomicInteger::get).toArray();
		}
	}

	@FunctionalInterface
	private interface FailureStrategy extends Serializable {
		void failOrNot(int trackingIndex);
	}

	private static class JoinedFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final FailureStrategy[] failureStrategies;

		private JoinedFailureStrategy(FailureStrategy ... failureStrategies) {
			this.failureStrategies = failureStrategies;
		}

		@Override
		public void failOrNot(int trackingIndex) {
			for (FailureStrategy failureStrategy : failureStrategies) {
				failureStrategy.failOrNot(trackingIndex);
			}
		}
	}

	private abstract static class AbstractRandomFailureStrategy implements FailureStrategy {
		private static final long serialVersionUID = 1L;

		private final CoinToss coin;

		private AbstractRandomFailureStrategy(CoinToss coin) {
			this.coin = coin;
		}

		@Override
		public void failOrNot(int trackingIndex) {
			if (coin.toss() && StaticFailureCounter.failOrNot()) {
				fail(trackingIndex);
				throw new FlinkRuntimeException("BAGA-BOOM!!! The user function generated test failure.");
			}
		}

		abstract void fail(int trackingIndex);
	}

	private static class RandomExceptionFailureStrategy extends AbstractRandomFailureStrategy {
		private static final long serialVersionUID = 1L;

		private RandomExceptionFailureStrategy(CoinToss coin) {
			super(coin);
		}

		@Override
		void fail(int trackingIndex) {
			StaticMapFailureTracker.mapFailure(trackingIndex);
			throw new FlinkRuntimeException("BAGA-BOOM!!! The user function generated test failure.");
		}
	}

	private static class RandomTaskExecutorFailureStrategy extends AbstractRandomFailureStrategy {
		private static final long serialVersionUID = 1L;

		private RandomTaskExecutorFailureStrategy(CoinToss coin) {
			super(coin);
		}

		@Override
		void fail(int trackingIndex) {
			Tuple2<String, Integer> nameWithIndex = TaskTrackingIndexConverter.trackingIndexToNameWithIndex(trackingIndex);
			// task executor of this task
			ResourceID taskExecutorId = client
				.getTaskExecutorIdOfSubtask(nameWithIndex.f0, nameWithIndex.f1)
				.orElseThrow(() -> new FlinkRuntimeException("Failed to define the task's executor id"));
			// track failure of all tasks of this task executor
			StaticMapFailureTracker.mapFailure(nameWithIndex.f1);
			// fail the task executor
			miniCluster.terminateTaskExecutor(taskExecutorId);
			getOrRethrow(miniCluster::startTaskExecutor);
		}
	}

	private enum TaskTrackingIndexConverter {
		;

		private static int taskNameToId(String name) {
			return Integer.parseInt(name.substring(
				name.indexOf(TASK_NAME_PREFIX) + TASK_NAME_PREFIX.length(),
				name.lastIndexOf(')')));
		}

		private static Tuple2<String, Integer> trackingIndexToNameWithIndex(int trackingIndex) {
			return Tuple2.of("MapPartition (" + TASK_NAME_PREFIX + trackingIndex + ')', 0);
		}
	}

	private static class CoinToss implements Serializable {
		private static final long serialVersionUID = 1L;
		private static final Random rnd = new Random();

		private final int probFraction;
		private final int probBase;

		private CoinToss(int probFraction, int probBase) {
			this.probFraction = probFraction;
			this.probBase = probBase;
		}

		private boolean toss() {
			int prob = rnd.nextInt(probBase) + 1;
			return prob <= probFraction;
		}
	}

	private static class TestPartitionMapper extends RichMapPartitionFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		private final int trackingIndex;
		private final FailureStrategy failureStrategy;

		private TestPartitionMapper(int trackingIndex, FailureStrategy failureStrategy) {
			this.trackingIndex = trackingIndex;
			this.failureStrategy = failureStrategy;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			StaticMapFailureTracker.mapRestart(trackingIndex);
		}

		@Override
		public void mapPartition(Iterable<Long> values, Collector<Long> out) {
			values.forEach(value -> {
				failOrNot();
				out.collect(value + 1);
			});
		}

		private void failOrNot() {
			failureStrategy.failOrNot(trackingIndex);
		}
	}

	private enum StaticFailureCounter {
		;

		private static final AtomicInteger failureNumber = new AtomicInteger(0);

		private static boolean failOrNot() {
			return failureNumber.incrementAndGet() < MAX_FAILURE_NUMBER;
		}

		private static void reset() {
			failureNumber.set(0);
		}
	}

	private static class MiniClusterClient extends RestClusterClient<StandaloneClusterId> {
		private MiniClusterClient(TestingMiniCluster miniCluster) throws Exception {
			super(createClientConfiguration(miniCluster), StandaloneClusterId.getInstance());
		}

		private static Configuration createClientConfiguration(MiniCluster miniCluster) {
			URI restAddress = getOrRethrow(miniCluster.getRestAddress()::get);
			Configuration restClientConfig = new Configuration();
			restClientConfig.setString(JobManagerOptions.ADDRESS, restAddress.getHost());
			restClientConfig.setInteger(RestOptions.PORT, restAddress.getPort());
			return new UnmodifiableConfiguration(restClientConfig);
		}

		private Optional<ResourceID> getTaskExecutorIdOfSubtask(String name, int subtaskIndex) {
			return getJobs()
				.stream()
				.map(jobId -> getTaskExecutorIdOfSubtaskForJob(jobId, name, subtaskIndex))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.findFirst();
		}

		private Optional<ResourceID> getTaskExecutorIdOfSubtaskForJob(JobID jobId, String name, int subtaskIndex) {
			return getOrRethrow(getJobDetails(jobId)::get)
				.getJobVertexInfos()
				.stream()
				.filter(jobVertexDetailsInfo -> name.equals(jobVertexDetailsInfo.getName()))
				.findFirst()
				.flatMap(vertexInfo ->
					getJobVertexDetailsInfo(jobId, vertexInfo.getJobVertexID())
						.getSubtasks()
						.stream()
						.filter(subtask -> subtask.getSubtaskIndex() == subtaskIndex)
						.findFirst()
						.flatMap(subtask -> getTaskManagerIdByAddress(subtask.getHost())));
		}

		private List<TaskWithDataPort> getTaskExecutorSubtasks(ResourceID taskExecutorId) {
			Map<Integer, ResourceID> taskManagerIdsByDataPort = getTaskManagerIdsByDataPort();
			List<TaskWithDataPort> allSubtasks = getSubtaskskWithDataPort();
			return allSubtasks
				.stream()
				.filter(subtask -> taskExecutorId.equals(taskManagerIdsByDataPort.get(subtask.dataPort)))
				.collect(Collectors.toList());
		}

		private List<TaskWithDataPort> getSubtaskskWithDataPort() {
			return getJobs()
				.stream()
				.flatMap(jobId -> getOrRethrow(getJobDetails(jobId)::get)
					.getJobVertexInfos()
					.stream()
					.map(info -> Tuple2.of(jobId, info)))
				.flatMap(vertexInfoWithJobId ->
					getJobVertexDetailsInfo(vertexInfoWithJobId.f0, vertexInfoWithJobId.f1.getJobVertexID())
						.getSubtasks()
						.stream()
						.map(subtask -> new TaskWithDataPort(vertexInfoWithJobId.f1.getName(), subtask)))
				.collect(Collectors.toList());
		}

		private Collection<JobID> getJobs() {
			JobIdsWithStatusOverview jobIds = getOrRethrow(
				sendRequest(JobIdsWithStatusesOverviewHeaders.getInstance(), EmptyMessageParameters.getInstance())::get);
			return jobIds
				.getJobsWithStatus()
				.stream()
				.map(JobIdWithStatus::getJobId)
				.collect(Collectors.toList());
		}

		private JobVertexDetailsInfo getJobVertexDetailsInfo(JobID jobId, JobVertexID jobVertexID) {
			JobVertexDetailsHeaders detailsHeaders = JobVertexDetailsHeaders.getInstance();
			JobVertexMessageParameters params = new JobVertexMessageParameters();
			params.jobPathParameter.resolve(jobId);
			params.jobVertexIdPathParameter.resolve(jobVertexID);
			return getOrRethrow(sendRequest(detailsHeaders, params)::get);
		}

		private Optional<ResourceID> getTaskManagerIdByAddress(String address) {
			return Optional.ofNullable(getTaskManagerIdsByDataPort().getOrDefault(parsePort(address), null));
		}

		private Map<Integer, ResourceID> getTaskManagerIdsByDataPort() {
			return getOrRethrow(sendRequest(TaskManagersHeaders.getInstance(), EmptyMessageParameters.getInstance())::get)
				.getTaskManagerInfos()
				.stream()
				.collect(Collectors.toMap(TaskManagerInfo::getDataPort, TaskManagerInfo::getResourceId));
		}

		private static int parsePort(String address) {
			//noinspection HardcodedFileSeparator
			return URI.create(address.contains("//") ? address : "http://" + address).getPort();
		}

		private static class TaskWithDataPort {
			private final String name;
			private final int subtaskIndex;
			private final int dataPort;

			private TaskWithDataPort(String name, VertexTaskDetail vertexTaskDetail) {
				this.name = name;
				this.subtaskIndex = vertexTaskDetail.getSubtaskIndex();
				this.dataPort = parsePort(vertexTaskDetail.getHost());
			}

			@Override
			public String toString() {
				return name + " (" + subtaskIndex + ", " + dataPort + ')';
			}
		}
	}
}
