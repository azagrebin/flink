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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.configuration.MemorySize;

import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ResourceProfileBudgetManager}.
 */
public class ResourceProfileBudgetManagerTest {

	@Test
	public void testReserve() {
		ResourceProfileBudgetManager budgetManager = new ResourceProfileBudgetManager(
			ResourceProfile.newBuilder().setCpuCores(1.0).setTaskHeapMemory(MemorySize.parse(100 + "m")).build());

		assertTrue(budgetManager.reserve(
			ResourceProfile.newBuilder().setCpuCores(0.7).setTaskHeapMemory(MemorySize.parse(70 + "m")).build()));
		assertThat(budgetManager.getAvailableBudget(),
			is(ResourceProfile.newBuilder().setCpuCores(0.3).setTaskHeapMemory(MemorySize.parse(30 + "m")).build()));
	}

	@Test
	public void testReserveFail() {
		ResourceProfileBudgetManager budgetManager = new ResourceProfileBudgetManager(
			ResourceProfile.newBuilder().setCpuCores(1.0).setTaskHeapMemory(MemorySize.parse(100 + "m")).build());

		assertFalse(budgetManager.reserve(
			ResourceProfile.newBuilder().setCpuCores(1.2).setTaskHeapMemory(MemorySize.parse(120 + "m")).build()));
		assertThat(budgetManager.getAvailableBudget(),
			is(ResourceProfile.newBuilder().setCpuCores(1.0).setTaskHeapMemory(MemorySize.parse(100 + "m")).build()));
	}

	@Test
	public void testRelease() {
		ResourceProfileBudgetManager budgetManager = new ResourceProfileBudgetManager(
			ResourceProfile.newBuilder().setCpuCores(1.0).setTaskHeapMemory(MemorySize.parse(100 + "m")).build());

		assertTrue(budgetManager.reserve(
			ResourceProfile.newBuilder().setCpuCores(0.7).setTaskHeapMemory(MemorySize.parse(70 + "m")).build()));
		assertTrue(budgetManager.release(
			ResourceProfile.newBuilder().setCpuCores(0.5).setTaskHeapMemory(MemorySize.parse(50 + "m")).build()));
		assertThat(budgetManager.getAvailableBudget(),
			is(ResourceProfile.newBuilder().setCpuCores(0.8).setTaskHeapMemory(MemorySize.parse(80 + "m")).build()));
	}

	@Test
	public void testReleaseFail() {
		ResourceProfileBudgetManager budgetManager = new ResourceProfileBudgetManager(
			ResourceProfile.newBuilder().setCpuCores(1.0).setTaskHeapMemory(MemorySize.parse(100 + "m")).build());

		assertTrue(budgetManager.reserve(
			ResourceProfile.newBuilder().setCpuCores(0.7).setTaskHeapMemory(MemorySize.parse(70 + "m")).build()));
		assertFalse(budgetManager.release(
			ResourceProfile.newBuilder().setCpuCores(0.8).setTaskHeapMemory(MemorySize.parse(80 + "m")).build()));
		assertThat(budgetManager.getAvailableBudget(),
			is(ResourceProfile.newBuilder().setCpuCores(0.3).setTaskHeapMemory(MemorySize.parse(30 + "m")).build()));
	}
}
