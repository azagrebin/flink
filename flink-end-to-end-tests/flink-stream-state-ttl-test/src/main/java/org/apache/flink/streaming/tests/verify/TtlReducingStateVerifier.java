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

package org.apache.flink.streaming.tests.verify;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import java.util.List;

class TtlReducingStateVerifier
	extends AbstractTtlStateVerifier<ReducingStateDescriptor<Integer>, ReducingState<Integer>, Integer, Integer, Integer> {
	TtlReducingStateVerifier() {
		super(new ReducingStateDescriptor<>(
			"TtlReducingStateVerifier",
			(ReduceFunction<Integer>) (value1, value2) -> value1 + value2,
			IntSerializer.INSTANCE));
	}

	@Override
	State createState(FunctionInitializationContext context) {
		return context.getKeyedStateStore().getReducingState(stateDesc);
	}

	@Override
	public TypeSerializer<Integer> getUpdateSerializer() {
		return IntSerializer.INSTANCE;
	}

	@Override
	public Integer generateRandomUpdate() {
		return RANDOM.nextInt(100);
	}

	@Override
	Integer getInternal(ReducingState<Integer> state) throws Exception {
		return state.get();
	}

	@Override
	void updateInternal(ReducingState<Integer> state, Integer update) throws Exception {
		state.add(update);
	}

	@Override
	Integer expected(List<TtlValue<Integer>> updates, long currentTimestamp) {
		if (updates.isEmpty()) {
			return null;
		}
		int acc = 0;
		long lastTs = updates.get(0).getUpdateTimestamp();
		for (TtlValue<Integer> update : updates) {
			if (expired(lastTs, update.getUpdateTimestamp())) {
				acc = 0;
			}
			acc += update.getValue();
			lastTs = update.getUpdateTimestamp();
		}
		return expired(lastTs, currentTimestamp) ? null : acc;
	}
}
