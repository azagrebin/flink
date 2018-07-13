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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

class TtlListStateVerifier
	extends AbstractTtlStateVerifier<ListStateDescriptor<String>, ListState<String>, List<String>, String, List<String>> {
	TtlListStateVerifier() {
		super(new ListStateDescriptor<>("TtlListStateVerifier", StringSerializer.INSTANCE));
	}

	@Override
	State createState(FunctionInitializationContext context) {
		return context.getKeyedStateStore().getListState(stateDesc);
	}

	@Override
	public TypeSerializer<String> getUpdateSerializer() {
		return StringSerializer.INSTANCE;
	}

	@Override
	public String generateRandomUpdate() {
		return randomString();
	}

	@Override
	List<String> getInternal(ListState<String> state) throws Exception {
		return StreamSupport.stream(state.get().spliterator(), false)
			.collect(Collectors.toList());
	}

	@Override
	void updateInternal(ListState<String> state, String update) throws Exception {
		state.add(update);
	}

	@Override
	List<String> expected(List<TtlValue<String>> updates, long currentTimestamp) {
		return updates.stream()
			.filter(u -> !expired(u.getUpdateTimestamp(), currentTimestamp))
			.map(TtlValue::getValue)
			.collect(Collectors.toList());
	}
}
