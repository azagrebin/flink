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

import java.nio.ByteBuffer;

class UnsafeMemorySegment extends DirectMemorySegment {
	private static final Runnable EMPTY_CLEANER = () -> {};

	private final Runnable cleaner;

	UnsafeMemorySegment(byte[] buffer, Object owner) {
		super(buffer, owner);
		cleaner = EMPTY_CLEANER;
	}

	UnsafeMemorySegment(int size, Object owner) {
		super(MemoryUtils.allocateUnsafe(size), size, owner);
		cleaner = MemoryUtils.createMemoryGcCleaner(this, address);
	}

	@Override
	public void free() {
		super.free();
		cleaner.run();
	}

	@Override
	public ByteBuffer getOffHeapBuffer() {
		// we need to keep reference to this segment from this created ByteBuffer view to avoid freeing memory
		// in GC cleaner
		return MemoryUtils.wrapUnsafeMemoryWithByteBuffer(address, size, this);
	}
}
