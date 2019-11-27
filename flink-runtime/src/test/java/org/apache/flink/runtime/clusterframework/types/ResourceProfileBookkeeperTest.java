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

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ResourceProfileBookkeeper}.
 */
public class ResourceProfileBookkeeperTest {

	@Test
	public void testReserve() {
		ResourceProfileBookkeeper bookkeeper = new ResourceProfileBookkeeper(new ResourceProfile(1.0, 100));
		assertTrue(bookkeeper.reserve(new ResourceProfile(0.7, 70)));
		Assert.assertThat(bookkeeper.getAvailableBudget(), is(new ResourceProfile(0.3, 30)));
	}

	@Test
	public void testReserveFail() {
		ResourceProfileBookkeeper bookkeeper = new ResourceProfileBookkeeper(new ResourceProfile(1.0, 100));
		assertFalse(bookkeeper.reserve(new ResourceProfile(1.2, 120)));
		assertThat(bookkeeper.getAvailableBudget(), is(new ResourceProfile(1.0, 100)));
	}

	@Test
	public void testRelease() {
		ResourceProfileBookkeeper bookkeeper = new ResourceProfileBookkeeper(new ResourceProfile(1.0, 100));
		assertTrue(bookkeeper.reserve(new ResourceProfile(0.7, 70)));
		assertTrue(bookkeeper.release(new ResourceProfile(0.5, 50)));
		assertThat(bookkeeper.getAvailableBudget(), is(new ResourceProfile(0.8, 80)));
	}

	@Test
	public void testReleaseFail() {
		ResourceProfileBookkeeper bookkeeper = new ResourceProfileBookkeeper(new ResourceProfile(1.0, 100));
		assertTrue(bookkeeper.reserve(new ResourceProfile(0.7, 70)));
		assertFalse(bookkeeper.release(new ResourceProfile(0.8, 80)));
		assertThat(bookkeeper.getAvailableBudget(), is(new ResourceProfile(0.3, 30)));
	}
}
