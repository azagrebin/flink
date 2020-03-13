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

package org.apache.flink.util;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ClassLoaderWithErrorHandler}.
 */
public class ClassLoaderWithErrorHandlerTest extends TestLogger {
	@Test
	public void testExceptionHandling() {
		RuntimeException expectedException = new RuntimeException("Expected exception");
		AtomicReference<Throwable> handledException = new AtomicReference<>();
		try (ClassLoaderWithErrorHandler classLoaderWithErrorHandler =
				new ClassLoaderWithErrorHandler(new ThrowingURLClassLoader(expectedException), handledException::set)) {
			classLoaderWithErrorHandler.loadClass("dummy.class");
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("Unexpected", e);
		} catch (Throwable t) {
			assertThat(handledException.get(), is(expectedException));
			assertThat(t, is(expectedException));
			return;
		}
		fail("The expected exception is not thrown");
	}

	private static class ThrowingURLClassLoader extends URLClassLoader {
		private final RuntimeException expectedException;

		private ThrowingURLClassLoader(RuntimeException expectedException) {
			super(new URL[]{});
			this.expectedException = expectedException;
		}

		@Override
		public Class<?> loadClass(String name) {
			throw expectedException;
		}
	}
}
