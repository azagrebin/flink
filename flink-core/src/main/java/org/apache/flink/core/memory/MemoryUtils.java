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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class for memory operations.
 */
@Internal
public class MemoryUtils {

	/** The "unsafe", which can be used to perform native memory accesses. */
	@SuppressWarnings("restriction")
	public static final sun.misc.Unsafe UNSAFE = getUnsafe();

	/** The native byte order of the platform on which the system currently runs. */
	public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

	private static final Constructor<?> DIRECT_BUFFER_CONSTRUCTOR = getDirectBufferPrivateConstructor();

	@SuppressWarnings("restriction")
	private static sun.misc.Unsafe getUnsafe() {
		try {
			Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			unsafeField.setAccessible(true);
			return (sun.misc.Unsafe) unsafeField.get(null);
		} catch (SecurityException e) {
			throw new RuntimeException("Could not access the sun.misc.Unsafe handle, permission denied by security manager.", e);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException("The static handle field in sun.misc.Unsafe was not found.");
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Bug: Illegal argument reflection access for static field.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
		} catch (Throwable t) {
			throw new RuntimeException("Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
		}
	}

	private static Constructor<? extends ByteBuffer> getDirectBufferPrivateConstructor() {
		//noinspection OverlyBroadCatchBlock
		try {
			return ByteBuffer.allocateDirect(1).getClass().getDeclaredConstructor(long.class, int.class, Object.class);
		} catch (NoSuchMethodException e) {
			ExceptionUtils.rethrow(
				e,
				"The private constructor java.nio.DirectByteBuffer.<init>(long, int) is not available.");
		} catch (SecurityException e) {
			ExceptionUtils.rethrow(
				e,
				"The private constructor java.nio.DirectByteBuffer.<init>(long, int) is not available, " +
					"permission denied by security manager");
		} catch (Throwable t) {
			ExceptionUtils.rethrow(
				t,
				"Unclassified error while trying to access private constructor " +
					"java.nio.DirectByteBuffer.<init>(long, int).");
		}
		throw new RuntimeException("unexpected to avoid returning null");
	}

	/** Should not be instantiated. */
	private MemoryUtils() {}

	public static AllocatedMemory allocateUnsafeWithAlignmentAndGcCleaner(Object owner, long size) {
		AllocatedMemory allocated = allocateUnsafeWithAlignment(size);
		return new AllocatedMemory(owner, allocated.getAddress(), allocated.getSize());
	}

	private static AllocatedMemory allocateUnsafeWithAlignment(long size) {
		int pageSize = UNSAFE.pageSize();
		long sizeWithAlignment = Math.max(1L, size + pageSize);

		long base = allocateUnsafe(sizeWithAlignment);

		UNSAFE.setMemory(base, sizeWithAlignment, (byte) 0);
		boolean needsAlignment = base % pageSize != 0;
		long alignment = needsAlignment ? pageSize - (base & (pageSize - 1)) : 0; // Round up to page boundary
		return new AllocatedMemory(null, base + alignment, sizeWithAlignment);
	}

	public static AllocatedMemory allocateUnsafeWithGcCleaner(Object owner, long size) {
		long address = allocateUnsafe(size);
		return new AllocatedMemory(owner, address, size);
	}

	public static long allocateUnsafe(long size) {
		return UNSAFE.allocateMemory(size);
	}

	public static Runnable createMemoryGcCleaner(Object owner, long address) {
		return createGcCleaner(owner, () -> releaseUnsafe(address));
	}

	@SuppressWarnings("UseOfSunClasses")
	private static Runnable createGcCleaner(Object owner, Runnable toClean) {
		return owner == null ? toClean : sun.misc.Cleaner.create(owner, toClean)::clean;
	}

	private static void releaseUnsafe(long address) {
		UNSAFE.freeMemory(address);
	}

	static ByteBuffer wrapUnsafeMemoryWithByteBuffer(long address, long size, Object owner) {
		//noinspection OverlyBroadCatchBlock
		try {
			return (ByteBuffer) DIRECT_BUFFER_CONSTRUCTOR.newInstance(address, size, owner);
		} catch (Throwable t) {
			ExceptionUtils.rethrow(t, "Failed to wrap unsafe off-heap memory with ByteBuffer");
		}
		throw new RuntimeException("unexpected to avoid returning null");
	}

	public static class AllocatedMemory {
		private final Object owner;
		private final long address;
		private final long size;
		private final Runnable cleaner;

		AllocatedMemory(Object owner, long address, long size) {
			this.owner = owner;
			this.address = address;
			this.size = size;
			this.cleaner = createMemoryGcCleaner(owner, address);
		}

		public Object getOwner() {
			return owner;
		}

		public long getAddress() {
			return address;
		}

		public long getSize() {
			return size;
		}

		public void release() {
			cleaner.run();
		}
	}
}
