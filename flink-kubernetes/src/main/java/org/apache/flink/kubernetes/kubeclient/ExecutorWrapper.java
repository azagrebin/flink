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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExecutorUtils;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The help class to get the executor to run asynchronous operations on. It could create a dedicated thread pool or
 * reuse an existing one.
 */
public class ExecutorWrapper {

	private ExecutorService internalExecutorService;
	private Executor executor;

	private ExecutorWrapper(Configuration flinkConfig) {
		internalExecutorService = Executors.newFixedThreadPool(
			flinkConfig.getInteger(KubernetesConfigOptions.CLIENT_ASYNC_THREAD_POOL_SIZE),
			new ExecutorThreadFactory("FlinkKubeClient-IO"));
	}

	private ExecutorWrapper(Executor executor) {
		this.executor = executor;
	}

	public static ExecutorWrapper createExecutorWrapper(Configuration flinkConfig) {
		return new ExecutorWrapper(flinkConfig);
	}

	public static ExecutorWrapper createExecutorWrapper(Executor executor) {
		return new ExecutorWrapper(executor);
	}

	Executor getExecutor() {
		return executor == null ? this.internalExecutorService : executor;
	}

	public void close() {
		if (internalExecutorService != null) {
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, this.internalExecutorService);
		}
	}
}
