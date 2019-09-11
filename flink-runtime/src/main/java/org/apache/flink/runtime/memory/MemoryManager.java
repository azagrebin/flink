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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.HybridMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.types.Either;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledOffHeapMemory;
import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;

/**
 * The memory manager governs the memory that Flink uses for sorting, hashing, and caching. Memory
 * is represented in segments of equal size. Operators allocate the memory by requesting a number
 * of memory segments.
 *
 * <p>The memory may be represented as on-heap byte arrays or as off-heap memory regions
 * (both via {@link HybridMemorySegment}). Which kind of memory the MemoryManager serves can
 * be passed as an argument to the initialization. Releasing a memory segment will make it re-claimable
 * by the garbage collector.
 */
public class MemoryManager {

	private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);
	/** The default memory page size. Currently set to 32 KiBytes. */
	public static final int DEFAULT_PAGE_SIZE = 32 * 1024;

	/** The minimal memory page size. Currently set to 4 KiBytes. */
	public static final int MIN_PAGE_SIZE = 4 * 1024;

	// ------------------------------------------------------------------------

	/** Memory segments allocated per memory owner. */
	private final Map<Object, Set<MemorySegment>> allocatedSegments;

	/** The size of the memory segments. */
	private final int pageSize;

	/** The initial total size, for verification. */
	private final int totalNumPages;

	/** The total size of the memory managed by this memory manager. */
	private final long memorySize;

	/** Number of slots of the task manager. */
	private final int numberOfSlots;

	/** Type of the managed memory. */
	private final MemoryType memoryType;

	private final KeyedBudget<MemoryType> budgetByType;

	/** Flag whether the close() has already been invoked. */
	private volatile boolean isShutDown;

	/**
	 * Creates a memory manager with the given capacity and given page size.
	 *
	 * @param memorySize The total size of the memory to be managed by this memory manager.
	 * @param numberOfSlots The number of slots of the task manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 * @param memoryType The type of memory (heap / off-heap) that the memory manager should allocate.
	 */
	public MemoryManager(
			long memorySize,
			int numberOfSlots,
			int pageSize,
			MemoryType memoryType) {
		sanityCheck(memorySize, pageSize, memoryType);

		this.allocatedSegments = new ConcurrentHashMap<>();
		this.memorySize = memorySize;
		this.numberOfSlots = numberOfSlots;
		this.pageSize = pageSize;
		this.totalNumPages = calculateTotalNumberOfPages(memorySize, pageSize);
		this.memoryType = memoryType;
		this.budgetByType = new KeyedBudget<>(Collections.singletonMap(memoryType, memorySize));

		LOG.debug(
			"Initialized MemoryManager with total memory size {}, number of slots {}, page size {} and memory type {}.",
			memorySize,
			numberOfSlots,
			pageSize,
			memoryType);
	}

	private static void sanityCheck(long memorySize, int pageSize, MemoryType memoryType) {
		Preconditions.checkNotNull(memoryType);
		Preconditions.checkArgument(memorySize > 0L, "Size of total memory must be positive.");
		Preconditions.checkArgument(
			pageSize >= MIN_PAGE_SIZE,
			"The page size must be at least %d bytes.", MIN_PAGE_SIZE);
		Preconditions.checkArgument(
			MathUtils.isPowerOf2(pageSize),
			"The given page size is not a power of two.");
	}

	private static int calculateTotalNumberOfPages(long memorySize, int pageSize) {
		long numPagesLong = memorySize / pageSize;
		Preconditions.checkArgument(
			numPagesLong <= Integer.MAX_VALUE,
			"The given number of memory bytes (%s) corresponds to more than MAX_INT pages.", memorySize);

		@SuppressWarnings("NumericCastThatLosesPrecision")
		int totalNumPages = (int) numPagesLong;
		Preconditions.checkArgument(totalNumPages >= 1, "The given amount of memory amounted to less than one page.");

		return totalNumPages;
	}

	// ------------------------------------------------------------------------
	//  Shutdown
	// ------------------------------------------------------------------------

	/**
	 * Shuts the memory manager down, trying to release all the memory it managed. Depending
	 * on implementation details, the memory does not necessarily become reclaimable by the
	 * garbage collector, because there might still be references to allocated segments in the
	 * code that allocated them from the memory manager.
	 */
	public void shutdown() {
		if (!isShutDown) {
			// mark as shutdown and release memory
			isShutDown = true;
			budgetByType.releaseAll();

			// go over all allocated segments and release them
			for (Set<MemorySegment> segments : allocatedSegments.values()) {
				for (MemorySegment seg : segments) {
					seg.free();
				}
				segments.clear();
			}
			allocatedSegments.clear();
		}
	}

	/**
	 * Checks whether the MemoryManager has been shut down.
	 *
	 * @return True, if the memory manager is shut down, false otherwise.
	 */
	@VisibleForTesting
	public boolean isShutdown() {
		return isShutDown;
	}

	/**
	 * Checks if the memory manager all memory available.
	 *
	 * @return True, if the memory manager is empty and valid, false if it is not empty or corrupted.
	 */
	@VisibleForTesting
	public boolean verifyEmpty() {
		return budgetByType.totalAvailableBudget() == memorySize;
	}

	// ------------------------------------------------------------------------
	//  Memory allocation and release
	// ------------------------------------------------------------------------

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param numPages The number of pages to allocate.
	 * @return A list with the memory segments.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	public List<MemorySegment> allocatePages(Object owner, int numPages) throws MemoryAllocationException {
		List<MemorySegment> segments = new ArrayList<>(numPages);
		allocatePages(owner, segments, numPages);
		return segments;
	}

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * @param owner The owner to associate with the memory segment, for the fallback release.
	 * @param target The list into which to put the allocated memory pages.
	 * @param numPages The number of pages to allocate.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory pages any more.
	 */
	public void allocatePages(
			Object owner,
			Collection<MemorySegment> target,
			int numPages) throws MemoryAllocationException {
		// sanity check
		Preconditions.checkNotNull(owner, "The memory owner must not be null.");
		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		// reserve array space, if applicable
		if (target instanceof ArrayList) {
			((ArrayList<MemorySegment>) target).ensureCapacity(numPages);
		}

		Either<Map<MemoryType, Long>, Long> acquiredBudget = budgetByType.acquirePagedBudget(numPages, pageSize);
		if (acquiredBudget.isRight()) {
			throw new MemoryAllocationException(
				String.format("Could not allocate %d pages. Only %d pages are remaining.", numPages, acquiredBudget.right()));
		}

		allocatedSegments.compute(owner, (o, currentSegmentsForOwner) -> {
			Set<MemorySegment> segmentsForOwner = currentSegmentsForOwner == null ?
				new HashSet<>(numPages) : currentSegmentsForOwner;
			for (int i = numPages; i > 0; i--) {
				MemorySegment segment = allocateManagedSegment(memoryType, owner);
				target.add(segment);
				segmentsForOwner.add(segment);
			}
			return segmentsForOwner;
		});

		Preconditions.checkState(!isShutDown, "Memory manager has been concurrently shut down.");
	}

	/**
	 * Tries to release the memory for the specified segment.
	 *
	 * <p>If the segment has already been released or is null, the request is simply ignored.
	 * The segment is only freed and made eligible for reclamation by the GC.
	 *
	 * @param segment The segment to be released.
	 * @throws IllegalArgumentException Thrown, if the given segment is of an incompatible type.
	 */
	public void release(MemorySegment segment) {
		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		// check if segment is null or has already been freed
		if (segment == null || segment.getOwner() == null) {
			return;
		}

		// remove the reference in the map for the owner
		try {
			allocatedSegments.computeIfPresent(segment.getOwner(), (o, segsForOwner) -> {
				segsForOwner.remove(segment);
				segment.free();
				budgetByType.releaseBudgetForKey(getSegmentType(segment), pageSize);
				//noinspection ReturnOfNull
				return segsForOwner.isEmpty() ? null : segsForOwner;
			});
		}
		catch (Throwable t) {
			throw new RuntimeException("Error removing book-keeping reference to allocated memory segment.", t);
		}
	}

	/**
	 * Tries to release many memory segments together.
	 *
	 * <p>The segment is only freed and made eligible for reclamation by the GC.
	 *
	 * @param segments The segments to be released.
	 * @throws NullPointerException Thrown, if the given collection is null.
	 * @throws IllegalArgumentException Thrown, id the segments are of an incompatible type.
	 */
	public void release(Collection<MemorySegment> segments) {
		if (segments == null) {
			return;
		}

		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		EnumMap<MemoryType, Long> releasedMemory = new EnumMap<>(MemoryType.class);

		// since concurrent modifications to the collection
		// can disturb the release, we need to try potentially multiple times
		boolean successfullyReleased = false;
		do {
			Iterator<MemorySegment> segmentsIterator = segments.iterator();

			//noinspection ProhibitedExceptionCaught
			try {
				MemorySegment segment = null;
				while (segment == null && segmentsIterator.hasNext()) {
					segment = segmentsIterator.next();
					if (segment.isFreed()) {
						segment = null;
					}
				}
				while (segment != null) {
					segment = releaseSegmentsForOwnerUntilNextOwner(segment, segmentsIterator, releasedMemory);
				}
				segments.clear();
				// the only way to exit the loop
				successfullyReleased = true;
			} catch (ConcurrentModificationException | NoSuchElementException e) {
				// this may happen in the case where an asynchronous
				// call releases the memory. fall through the loop and try again
			}
		} while (!successfullyReleased);

		budgetByType.releaseBudgetForKeys(releasedMemory);
	}

	private MemorySegment releaseSegmentsForOwnerUntilNextOwner(
			MemorySegment firstSeg,
			Iterator<MemorySegment> segmentsIterator,
			EnumMap<MemoryType, Long> releasedMemory) {
		AtomicReference<MemorySegment> nextOwnerMemorySegment = new AtomicReference<>();
		Object owner = firstSeg.getOwner();
		allocatedSegments.compute(owner, (o, segsForOwner) -> {
			freeSegment(firstSeg, segsForOwner, releasedMemory);
			while (segmentsIterator.hasNext()) {
				MemorySegment segment = segmentsIterator.next();
				try {
					if (segment == null || segment.isFreed()) {
						continue;
					}
					Object nextOwner = segment.getOwner();
					if (nextOwner != owner) {
						nextOwnerMemorySegment.set(segment);
						break;
					}
					freeSegment(segment, segsForOwner, releasedMemory);
				} catch (Throwable t) {
					throw new RuntimeException(
						"Error removing book-keeping reference to allocated memory segment.", t);
				}
			}
			//noinspection ReturnOfNull
			return segsForOwner == null || segsForOwner.isEmpty() ? null : segsForOwner;
		});
		return nextOwnerMemorySegment.get();
	}

	private void freeSegment(
			MemorySegment segment,
			@Nullable Collection<MemorySegment> segments,
			EnumMap<MemoryType, Long> releasedMemory) {
		segment.free();
		if (segments != null && segments.remove(segment)) {
			releaseSegment(segment, releasedMemory);
		}
	}

	/**
	 * Releases all memory segments for the given owner.
	 *
	 * @param owner The owner memory segments are to be released.
	 */
	public void releaseAll(Object owner) {
		if (owner == null) {
			return;
		}

		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");

		// get all segments
		Set<MemorySegment> segments = allocatedSegments.remove(owner);

		// all segments may have been freed previously individually
		if (segments == null || segments.isEmpty()) {
			return;
		}

		// free each segment
		EnumMap<MemoryType, Long> releasedMemory = new EnumMap<>(MemoryType.class);
		for (MemorySegment segment : segments) {
			segment.free();
			releaseSegment(segment, releasedMemory);
		}
		budgetByType.releaseBudgetForKeys(releasedMemory);

		segments.clear();
	}

	// ------------------------------------------------------------------------
	//  Properties, sizes and size conversions
	// ------------------------------------------------------------------------

	/**
	 * Gets the size of the pages handled by the memory manager.
	 *
	 * @return The size of the pages handled by the memory manager.
	 */
	public int getPageSize() {
		return pageSize;
	}

	/**
	 * Returns the total size of memory handled by this memory manager.
	 *
	 * @return The total size of memory.
	 */
	public long getMemorySize() {
		return memorySize;
	}

	/**
	 * Computes to how many pages the given number of bytes corresponds. If the given number of bytes is not an
	 * exact multiple of a page size, the result is rounded down, such that a portion of the memory (smaller
	 * than the page size) is not included.
	 *
	 * @param fraction the fraction of the total memory per slot
	 * @return The number of pages to which
	 */
	public int computeNumberOfPages(double fraction) {
		if (fraction <= 0 || fraction > 1) {
			throw new IllegalArgumentException("The fraction of memory to allocate must within (0, 1].");
		}

		//noinspection NumericCastThatLosesPrecision
		return (int) (totalNumPages * fraction / numberOfSlots);
	}

	private MemorySegment allocateManagedSegment(MemoryType memoryType, Object owner) {
		switch (memoryType) {
			case HEAP:
				return allocateUnpooledSegment(pageSize, owner);
			case OFF_HEAP:
				return allocateUnpooledOffHeapMemory(pageSize, owner);
			default:
				throw new IllegalArgumentException("unrecognized memory type: " + memoryType);
		}
	}

	private void releaseSegment(MemorySegment segment, EnumMap<MemoryType, Long> releasedMemory) {
		releasedMemory.compute(getSegmentType(segment), (t, v) -> v == null ? pageSize : v + pageSize);
	}

	private static MemoryType getSegmentType(MemorySegment segment) {
		return segment.isOffHeap() ? MemoryType.OFF_HEAP : MemoryType.HEAP;
	}
}
