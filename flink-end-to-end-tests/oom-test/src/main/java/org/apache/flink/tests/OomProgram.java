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

package org.apache.flink.tests;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/** Dummy program to simulate {@link OutOfMemoryError}. */
public class OomProgram {
	private OomProgram() {
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env
			.addSource(new DummySourceFunction())
			.map(new OomMapFunction(5))
			.print();

		env.execute("State TTL test job");
	}

	private static class DummySourceFunction implements SourceFunction<String> {
		private static final long serialVersionUID = 4344206963974038249L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<String> ctx) throws InterruptedException {
			while (running) {
				ctx.collect("dummy");
				//noinspection BusyWait
				Thread.sleep(100);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class OomMapFunction implements MapFunction<String, String> {
		private static final long serialVersionUID = 7699090086364487119L;

		private final int oomAfterNumberOfMessages;
		private int messageCount;

		private OomMapFunction(int oomAfterNumberOfMessages) {
			this.oomAfterNumberOfMessages = oomAfterNumberOfMessages;
		}

		@Override
		public String map(String value) {
			if (messageCount >= oomAfterNumberOfMessages) {
				throw new OutOfMemoryError("Metaspace");
			}
			messageCount++;
			return value;
		}
	}
}
