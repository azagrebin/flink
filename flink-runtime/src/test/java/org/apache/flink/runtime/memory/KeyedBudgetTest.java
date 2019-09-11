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

package org.apache.flink.runtime.memory;

import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test suite for {@link KeyedBudget}.
 */
@SuppressWarnings("MagicNumber")
public class KeyedBudgetTest {
	private static final String[] TEST_KEYS = {"k1", "k2", "k3", "k4"};
	private static final long[] TEST_BUDGETS = {15, 17, 22, 11};

	@Test
	public void testSuccessfulAcquisitionForKey() {
		KeyedBudget<String> keyedBudget = createSimpleKeyedBudget();

		long acquired = keyedBudget.acquireBudgetForKey("k1", 10L);

		assertThat(acquired, is(10L));
		checkOneKeyBudgetChange(keyedBudget, "k1", 5L);
	}

	@Test
	public void testFailedAcquisitionForKey() {
		KeyedBudget<String> keyedBudget = createSimpleKeyedBudget();

		long acquired = keyedBudget.acquireBudgetForKey("k1", 20L);

		assertThat(acquired, is(15L));
		checkOneKeyBudgetChange(keyedBudget, "k1", 15L);
	}

	@Test
	public void testSuccessfulReleaseForKey() {
		KeyedBudget<String> keyedBudget = createSimpleKeyedBudget();

		keyedBudget.acquireBudgetForKey("k1", 10L);
		keyedBudget.releaseBudgetForKey("k1", 5L);

		checkOneKeyBudgetChange(keyedBudget, "k1", 10L);
	}

	@Test
	public void testFailedReleaseForKey() {
		KeyedBudget<String> keyedBudget = createSimpleKeyedBudget();

		keyedBudget.acquireBudgetForKey("k1", 10L);
		try {
			keyedBudget.releaseBudgetForKey("k1", 15L);
			fail("IllegalStateException is expected to fail over-sized release");
		} catch (IllegalStateException e) {
			// expected
		}

		checkOneKeyBudgetChange(keyedBudget, "k1", 5L);
	}

	@Test
	public void testSuccessfulAcquisitionForKeys() {
		KeyedBudget<String> keyedBudget = createSimpleKeyedBudget();

		Either<Map<String, Long>, Long> acquired =
			keyedBudget.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, 5);

		assertThat(acquired.isLeft(), is(true));
		assertThat(acquired.left().values().stream().mapToLong(b -> b).sum(), is(4L));

		assertThat(keyedBudget.availableBudgetForKey("k1"), is(15L));
		assertThat(keyedBudget.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(19L));
		assertThat(keyedBudget.totalAvailableBudget(), is(45L));
	}

	@Test
	public void testSuccessfulReleaseForKeys() {
		KeyedBudget<String> keyedBudget = createSimpleKeyedBudget();

		keyedBudget.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, 8);
		keyedBudget.releaseBudgetForKeys(createdBudgetMap(new String[] {"k2", "k3"}, new long[] {7, 10}));

		assertThat(keyedBudget.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(24L));
		assertThat(keyedBudget.availableBudgetForKeys(Arrays.asList("k1", "k4")), is(26L));
		assertThat(keyedBudget.totalAvailableBudget(), is(50L));
	}

	@Test
	public void testSuccessfulReleaseForKeysWithMixedRequests() {
		KeyedBudget<String> keyedBudget = createSimpleKeyedBudget();

		keyedBudget.acquirePagedBudgetForKeys(Arrays.asList("k2", "k3"), 4, 8);
		keyedBudget.acquirePagedBudgetForKeys(Arrays.asList("k1", "k4"), 6, 3);
		keyedBudget.releaseBudgetForKeys(createdBudgetMap(new String[] {"k2", "k3"}, new long[] {7, 10}));

		assertThat(keyedBudget.availableBudgetForKeys(Arrays.asList("k2", "k3")), is(24L));
		assertThat(keyedBudget.availableBudgetForKeys(Arrays.asList("k1", "k4")), is(8L));
		assertThat(keyedBudget.totalAvailableBudget(), is(32L));
	}

	private static void checkOneKeyBudgetChange(
			KeyedBudget<String> keyedBudget,
			@SuppressWarnings("SameParameterValue") String key,
			long budget) {
		long totalExpectedBudget = 0L;
		for (int i = 0; i < TEST_KEYS.length; i++) {
			long expectedBudget = TEST_KEYS[i].equals(key) ? budget : TEST_BUDGETS[i];
			assertThat(keyedBudget.availableBudgetForKey(TEST_KEYS[i]), is(expectedBudget));
			totalExpectedBudget += expectedBudget;
		}
		assertThat(keyedBudget.maxTotalBudget(), is(LongStream.of(TEST_BUDGETS).sum()));
		assertThat(keyedBudget.totalAvailableBudget(), is(totalExpectedBudget));
	}

	private static KeyedBudget<String> createSimpleKeyedBudget() {
		return new KeyedBudget<>(createdBudgetMap(TEST_KEYS, TEST_BUDGETS));
	}

	private static Map<String, Long> createdBudgetMap(String[] keys, long[] budgets) {
		Preconditions.checkArgument(keys.length == budgets.length);
		Map<String, Long> keydBudgets = new HashMap<>();
		for (int i = 0; i < keys.length; i++) {
			keydBudgets.put(keys[i], budgets[i]);
		}
		return keydBudgets;
	}
}
