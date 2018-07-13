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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateTtlConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** TTL state verifier interface. */
public interface TtlStateVerifier<UV, GV> {
	List<TtlStateVerifier<?, ?>> VERIFIERS = Arrays.asList(
		new TtlValueStateVerifier(),
		new TtlListStateVerifier(),
		new TtlMapStateVerifier(),
		new TtlAggregatingStateVerifier(),
		new TtlReducingStateVerifier(),
		new TtlFoldingStateVerifier()
	);

	Map<String, TtlStateVerifier<?, ?>> VERIFIERS_BY_NAME =
		VERIFIERS.stream().collect(Collectors.toMap(TtlStateVerifier::getId, v -> v));

	default String getId() {
		return this.getClass().getSimpleName();
	}

	State createState(FunctionInitializationContext context, StateTtlConfiguration ttlConfig);

	TypeSerializer<UV> getUpdateSerializer();

	UV generateRandomUpdate();

	GV get(State state) throws Exception;

	void update(State state, Object update) throws Exception;

	boolean verify(TtlVerificationContext<?, ?> verificationContext, Time precision);
}
