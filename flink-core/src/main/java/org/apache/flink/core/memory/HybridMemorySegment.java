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

import java.nio.ByteBuffer;

/**
 * This class represents a piece of memory managed by Flink. The memory can be on-heap or off-heap,
 * this is transparently handled by this class.
 *
 * <p>This class specializes byte access and byte copy calls for heap memory, while reusing the
 * multi-byte type accesses and cross-segment operations from the MemorySegment.
 *
 * <p>This class subsumes the functionality of the {@link org.apache.flink.core.memory.HeapMemorySegment},
 * but is a bit less efficient for operations on individual bytes.
 *
 * <p>Note that memory segments should usually not be allocated manually, but rather through the
 * {@link MemorySegmentFactory}.
 */
@Internal
public final class HybridMemorySegment extends DirectMemorySegment {

	/**
	 * The direct byte buffer that allocated the off-heap memory. This memory segment holds a
	 * reference to that buffer, so as long as this memory segment lives, the memory will not be
	 * released.
	 */
	private final ByteBuffer offHeapBuffer;

	/**
	 * Creates a new memory segment that represents the memory backing the given direct byte buffer.
	 * Note that the given ByteBuffer must be direct {@link java.nio.ByteBuffer#allocateDirect(int)},
	 * otherwise this method with throw an IllegalArgumentException.
	 *
	 * <p>The owner referenced by this memory segment is null.
	 *
	 * @param buffer The byte buffer whose memory is represented by this memory segment.
	 * @throws IllegalArgumentException Thrown, if the given ByteBuffer is not direct.
	 */
	HybridMemorySegment(ByteBuffer buffer) {
		this(buffer, null);
	}

	/**
	 * Creates a new memory segment that represents the memory backing the given direct byte buffer.
	 * Note that the given ByteBuffer must be direct {@link java.nio.ByteBuffer#allocateDirect(int)},
	 * otherwise this method with throw an IllegalArgumentException.
	 *
	 * <p>The memory segment references the given owner.
	 *
	 * @param buffer The byte buffer whose memory is represented by this memory segment.
	 * @param owner The owner references by this memory segment.
	 * @throws IllegalArgumentException Thrown, if the given ByteBuffer is not direct.
	 */
	HybridMemorySegment(ByteBuffer buffer, Object owner) {
		super(checkBufferAndGetAddress(buffer), buffer.capacity(), owner);
		this.offHeapBuffer = buffer;
	}

	/**
	 * Creates a new memory segment that represents the memory of the byte array.
	 *
	 * <p>The owner referenced by this memory segment is null.
	 *
	 * @param buffer The byte array whose memory is represented by this memory segment.
	 */
	HybridMemorySegment(byte[] buffer) {
		this(buffer, null);
	}

	/**
	 * Creates a new memory segment that represents the memory of the byte array.
	 *
	 * <p>The memory segment references the given owner.
	 *
	 * @param buffer The byte array whose memory is represented by this memory segment.
	 * @param owner The owner references by this memory segment.
	 */
	HybridMemorySegment(byte[] buffer, Object owner) {
		super(buffer, owner);
		this.offHeapBuffer = null;
	}

	// -------------------------------------------------------------------------
	//  MemorySegment operations
	// -------------------------------------------------------------------------

	/**
	 * Gets the buffer that owns the memory of this memory segment.
	 *
	 * @return The byte buffer that owns the memory of this memory segment.
	 */
	@Override
	public ByteBuffer getOffHeapBuffer() {
		if (offHeapBuffer != null) {
			return offHeapBuffer;
		} else {
			throw new IllegalStateException("Memory segment does not represent off heap memory");
		}
	}

	private static long checkBufferAndGetAddress(ByteBuffer buffer) {
		if (buffer == null) {
			throw new NullPointerException("buffer is null");
		}
		if (!buffer.isDirect()) {
			throw new IllegalArgumentException("Can't initialize from non-direct ByteBuffer.");
		}
		return getAddress(buffer);
	}
}
