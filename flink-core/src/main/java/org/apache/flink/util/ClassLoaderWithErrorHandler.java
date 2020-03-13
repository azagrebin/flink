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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.function.Consumer;

/**
 * This {@link URLClassLoader} decorator accepts a custom exception handler.
 *
 * <p>The handler is called if an exception occurs in the {@link #loadClass(String, boolean)} of inner class loader.
 */
public class ClassLoaderWithErrorHandler extends URLClassLoader {
	public static final Consumer<Throwable> EMPTY_EXCEPTION_HANDLER = classLoadingException -> {};
	private static final URL[] EMPTY_URL_ARRAY = {};

	private final URLClassLoader inner;
	private final Consumer<Throwable> classLoadingExceptionHandler;

	public ClassLoaderWithErrorHandler(
			URLClassLoader inner,
			Consumer<Throwable> classLoadingExceptionHandler) {
		super(EMPTY_URL_ARRAY, inner.getParent());
		this.inner = inner;
		this.classLoadingExceptionHandler = classLoadingExceptionHandler;
	}

	@Override
	protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
		synchronized (getClassLoadingLock(name)) {
			final Class<?> loadedClass = findLoadedClass(name);
			if (loadedClass != null) {
				return resolveIfNeeded(resolve, loadedClass);
			}

			try {
				return resolveIfNeeded(resolve, inner.loadClass(name));
			} catch (Throwable classLoadingException) {
				classLoadingExceptionHandler.accept(classLoadingException);
				throw classLoadingException;
			}
		}
	}

	private Class<?> resolveIfNeeded(final boolean resolve, final Class<?> loadedClass) {
		if (resolve) {
			resolveClass(loadedClass);
		}

		return loadedClass;
	}

	@Override
	public URL getResource(final String name) {
		return inner.getResource(name);
	}

	@Override
	public Enumeration<URL> getResources(final String name) throws IOException {
		return inner.getResources(name);
	}

	@Override
	public InputStream getResourceAsStream(String name) {
		return inner.getResourceAsStream(name);
	}

	@Override
	public void close() throws IOException {
		super.close();
		inner.close();
	}
}
