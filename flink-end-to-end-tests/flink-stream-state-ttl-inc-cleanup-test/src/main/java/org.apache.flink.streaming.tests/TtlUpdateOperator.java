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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.tests.verify.TtlStateVerifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Update state with TTL for each verifier and log state size.
 *
 * <p>This operator outputs string messages if size of state exceeds {code@ maxAllowedStateSize}.
 */
class TtlUpdateOperator
	extends AbstractStreamOperator<String>
	implements OneInputStreamOperator<TtlStateUpdate, String> {

	private static final Logger LOG = LoggerFactory.getLogger(TtlUpdateOperator.class);
	private static final long serialVersionUID = 2155682100112401673L;

	@Nonnull
	private final StateTtlConfig ttlConfig;
	private final long reportStatAfterUpdatesNum;
	private final int maxAllowedStateSize;

	private transient long updateNum = 0;
	private transient Map<String, State> states;

	TtlUpdateOperator(@Nonnull StateTtlConfig ttlConfig, long reportStatAfterUpdatesNum, int maxAllowedStateSize) {
		this.ttlConfig = ttlConfig;
		this.reportStatAfterUpdatesNum = reportStatAfterUpdatesNum;
		this.maxAllowedStateSize = maxAllowedStateSize;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		states = TtlStateVerifier.VERIFIERS.stream()
			.collect(Collectors.toMap(TtlStateVerifier::getId, v -> v.createState(context, ttlConfig)));
	}

	@Override
	public void processElement(StreamRecord<TtlStateUpdate> element) throws Exception {
		TtlStateUpdate updates = element.getValue();
		for (TtlStateVerifier<?, ?> verifier : TtlStateVerifier.VERIFIERS) {
			State state = states.get(verifier.getId());
			Object update = updates.getUpdate(verifier.getId());
			if (update.hashCode() % 2 == 0) {
				verifier.update(state, update);
			} else {
				verifier.get(state);
			}
			updateNum++;
			if (updateNum % reportStatAfterUpdatesNum == 0) {
				String verifierName = verifier.getClass().getSimpleName();
				long keyNum = getKeyedStateBackend().getKeys(verifierName, VoidNamespace.INSTANCE).count();
				LOG.info("{}: Key number is {} after {} updates", verifierName, keyNum, updateNum);
				if (keyNum > maxAllowedStateSize) {
					output.collect(new StreamRecord<>("State oversize " + keyNum));
				}
			}
		}
	}
}
