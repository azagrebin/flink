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

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

import static org.apache.flink.streaming.tests.DataStreamAllroundTestJobFactory.setupEnvironment;

/**
 * A test job for State TTL feature.
 *
 * <p>The test pipeline does the following:
 * - activates incremental cleanup for state with TTL.
 * - generates random keyed state updates for each state TTL verifier (state type)
 * - performs update of created state with TTL for each verifier
 * - checks that state is being cleaned up and its size does not exceed configured max
 *
 * <p>Program parameters:
 * <ul>
 *     <li>update_generator_source.keyspace (int, default - 100): Number of different keys for updates emitted by the update generator.</li>
 *     <li>update_generator_source.sleep_time (long, default - 0): Milliseconds to sleep after emitting updates in the update generator. Set to 0 to disable sleeping.</li>
 *     <li>update_generator_source.sleep_after_elements (long, default - 0): Number of updates to emit before sleeping in the update generator. Set to 0 to disable sleeping.</li>
 *     <li>state_ttl_verifier.ttl_milli (long, default - 1000): State time-to-live.</li>
 *     <li>report_stat.after_updates_num (long, default - 200): Report state update statistics after certain number of updates (average update chain length and clashes).</li>
 *     <li>rmax.allowed.state.size (long, default - 40): Max observed size for state with TTL to check.</li>
 * </ul>
 */
public class DataStreamStateTtlIncCleanupTestProgram {
	private static final ConfigOption<Integer> MAX_ALLOWED_STATE_SIZE = ConfigOptions
		.key("max.allowed.state.size")
		.defaultValue(40);

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		setupEnvironment(env, pt);
		TtlTestConfig config = TtlTestConfig.fromArgs(pt);

		int maxAllowedStateSize = pt.getInt(MAX_ALLOWED_STATE_SIZE.key(), MAX_ALLOWED_STATE_SIZE.defaultValue());

		StateTtlConfig ttlConfig = StateTtlConfig
			.newBuilder(config.ttl)
			.cleanupIncrementally(4, true)
			.build();

		env
			.addSource(new TtlStateUpdateSource(config.keySpace, config.sleepAfterElements, config.sleepTime))
			.name("TtlStateUpdateSource")
			.keyBy(TtlStateUpdate::getKey)
			.transform("TtlUpdateOperator", BasicTypeInfo.STRING_TYPE_INFO,
				new TtlUpdateOperator(ttlConfig, config.reportStatAfterUpdatesNum, maxAllowedStateSize))
			.addSink(new PrintSinkFunction<>())
			.name("PrintStateOversize");

		env.execute("State TTL incremental cleanup test job");
	}
}
