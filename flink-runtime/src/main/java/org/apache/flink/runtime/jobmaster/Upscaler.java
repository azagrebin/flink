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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

class Upscaler {
	private final long debounceTimeoutMilli;
	private final RescalingOperation rescalingOperation;
	private final RescalingStrategy rescalingStrategy;
	private final ScheduledExecutor scheduledExecutor;
	private final Executor rescalingExecutor;
	private int currentParallelism;
	private ResourceManagerGateway resourceManagerGateway;
	private ScheduledFuture<?> upscalingAttempts;

	Upscaler(
		long debounceTimeoutMilli,
		RescalingOperation rescalingOperation,
		RescalingStrategy rescalingStrategy,
		ScheduledExecutor scheduledExecutor,
		Executor rescalingExecutor) {
		this.debounceTimeoutMilli = debounceTimeoutMilli;
		this.rescalingOperation = rescalingOperation;
		this.rescalingStrategy = rescalingStrategy;
		this.scheduledExecutor = scheduledExecutor;
		this.rescalingExecutor = rescalingExecutor;
		this.currentParallelism = Integer.MAX_VALUE;
	}

	public void setCurrentParallelism(int currentParallelism) {
		this.currentParallelism = currentParallelism;
	}

	void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
		this.resourceManagerGateway = checkNotNull(resourceManagerGateway);
		upscalingAttempts = startUpscalingAttempts(debounceTimeoutMilli);
	}

	void disconnectResourceManager() {
		upscalingAttempts.cancel(false);
		this.resourceManagerGateway = null;
	}

	private ScheduledFuture<?> startUpscalingAttempts(long debounceTimeoutMilli) {
		return scheduledExecutor.scheduleWithFixedDelay(
			this::tryToUpscale, 0L, debounceTimeoutMilli, TimeUnit.MILLISECONDS);
	}

	private void tryToUpscale() {
		rescalingExecutor.execute(() -> {
				if (resourceManagerGateway != null) {
					resourceManagerGateway
						.requestResourceOverview(Time.milliseconds(debounceTimeoutMilli))
						.thenAcceptAsync(resourceOverview -> {
							int slotsNumber = resourceOverview.getNumberRegisteredSlots();
							int newParallelism = rescalingStrategy.calcMaxParallelism(slotsNumber);
							if (newParallelism > currentParallelism) {
								rescalingOperation.rescale(newParallelism);
							}
						}, rescalingExecutor);
				}
			}
		);
	}

	@FunctionalInterface
	interface RescalingOperation {
		CompletableFuture<Acknowledge> rescale(int newParallelism);
	}

	@FunctionalInterface
	interface RescalingStrategy {
		int calcMaxParallelism(int slotsNumber);
	}

	static class RescalingStrategyImpl implements RescalingStrategy {
		private final JobGraph jobGraph;
		private final int minParallelism;

		RescalingStrategyImpl(JobGraph jobGraph) {
			this(jobGraph, 1);
		}

		RescalingStrategyImpl(JobGraph jobGraph, int minParallelism) {
			Preconditions.checkArgument(minParallelism > 0);
			this.jobGraph = jobGraph;
			this.minParallelism = minParallelism;
		}

		@Override
		public int calcMaxParallelism(int slotsNumber) {
			Set<SlotSharingGroupId> slotSharingGroups = new HashSet<>();
			for (JobVertex jobVertex : jobGraph.getVertices()) {
				slotSharingGroups.add(jobVertex.getSlotSharingGroup().getSlotSharingGroupId());
			}
			int maxPossibleParallelism = slotsNumber / slotSharingGroups.size();
			return maxPossibleParallelism >= minParallelism ? maxPossibleParallelism : 0;
		}
	}
}
