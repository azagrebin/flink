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
import java.util.ConcurrentModificationException;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledOffHeapMemory;
import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;

/**
 * The memory manager governs the memory that Flink uses for sorting, hashing, and caching. Memory is represented
 * either in {@link MemorySegment}s of equal size and arbitrary type or in reserved chunks of certain size and {@link MemoryType}.
 * Operators allocate the memory either by requesting a number of memory segments or by reserving chunks.
 * Any allocated memory has to be released to be reused later.
 *
 * <p>Which {@link MemoryType}s the MemoryManager serves and their total sizes can be passed as an argument
 * to the constructor.
 *
 * <p>The memory segments may be represented as on-heap byte arrays or as off-heap memory regions
 * (both via {@link HybridMemorySegment}). Releasing a memory segment will make it re-claimable
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

	/** Reserved memory per memory owner. */
	private final Map<Object, Map<MemoryType, Long>> reservedMemory;

	/** The size of the memory segments. */
	private final int pageSize;

	/** The initial total size, for verification. */
	private final int totalNumPages;

	/** Number of slots of the task manager. */
	private final int numberOfSlots;

	private final KeyedBudget<MemoryType> budgetByType;

	/** Flag whether the close() has already been invoked. */
	private volatile boolean isShutDown;

	/**
	 * Creates a memory manager with the given memory types, capacity and given page size.
	 *
	 * @param memorySizeByType The total size of the memory to be managed by this memory manager for each type (heap / off-heap).
	 * @param numberOfSlots The number of slots of the task manager.
	 * @param pageSize The size of the pages handed out by the memory manager.
	 */
	public MemoryManager(
			Map<MemoryType, Long> memorySizeByType,
			int numberOfSlots,
			int pageSize) {
		for (Entry<MemoryType, Long> sizeForType : memorySizeByType.entrySet()) {
			sanityCheck(sizeForType.getValue(), pageSize, sizeForType.getKey());
		}

		this.allocatedSegments = new ConcurrentHashMap<>();
		this.reservedMemory = new ConcurrentHashMap<>();
		this.numberOfSlots = numberOfSlots;
		this.pageSize = pageSize;
		this.totalNumPages = calculateTotalNumberOfPages(memorySizeByType, pageSize);
		this.budgetByType = new KeyedBudget<>(memorySizeByType);

		LOG.debug(
			"Initialized MemoryManager with total memory size {} ({}), number of slots {}, page size {}.",
			budgetByType.totalAvailableBudget(),
			memorySizeByType,
			numberOfSlots,
			pageSize);
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

	private static int calculateTotalNumberOfPages(Map<MemoryType, Long> memorySizeByType, int pageSize) {
		long numPagesLong = 0L;
		for (long sizeForType : memorySizeByType.values()) {
			numPagesLong += sizeForType / pageSize;
		}
		Preconditions.checkArgument(
			numPagesLong <= Integer.MAX_VALUE,
			"The given number of memory bytes (%d: %s) corresponds to more than MAX_INT pages.",
			numPagesLong,
			memorySizeByType);

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
			reservedMemory.clear();
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
		return budgetByType.totalAvailableBudget() == budgetByType.maxTotalBudget();
	}

	// ------------------------------------------------------------------------
	//  Memory allocation and release
	// ------------------------------------------------------------------------

	/**
	 * Allocates a set of memory segments from this memory manager.
	 *
	 * <p>The returned segments can have any memory type. The total allocated memory for each type will not exceed its
	 * size limit, announced in the constructor.
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
	 * <p>The returned segments can have any memory type. The total allocated memory for each type will not exceed its
	 * size limit, announced in the constructor.
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

		// in the case of pre-allocated memory, the 'numNonAllocatedPages' is zero, in the
		// lazy case, the 'freeSegments.size()' is zero.
		Either<Map<MemoryType, Long>, Long> acquiredBudget = budgetByType.acquirePagedBudget(numPages, pageSize);
		if (acquiredBudget.isRight()) {
			throw new MemoryAllocationException(
				String.format("Could not allocate %d pages. Only %d pages are remaining.", numPages, acquiredBudget.right()));
		}

		allocatedSegments.compute(owner, (o, currentSegmentsForOwner) -> {
			Set<MemorySegment> segmentsForOwner = currentSegmentsForOwner == null ?
				new HashSet<>(numPages) : currentSegmentsForOwner;
			for (MemoryType memoryType : acquiredBudget.left().keySet()) {
				for (long i = acquiredBudget.left().get(memoryType); i > 0; i--) {
					MemorySegment segment = allocateManagedSegment(memoryType, owner);
					target.add(segment);
					segmentsForOwner.add(segment);
				}
			}
			return segmentsForOwner;
		});

		Preconditions.checkState(!isShutDown, "Memory manager has been concurrently shut down.");
	}

	/**
	 * Tries to release the memory for the specified segment.
	 *
	 * <p>If the segment has already been released or is null, the request is simply ignored.
	 * The segment is only freed and made eligible for reclamation by the GC. The segment will be returned to
	 * the memory pool of its type, increasing its available limit for the later allocations.
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
	 * <p>The segment is only freed and made eligible for reclamation by the GC. Each segment will be returned to
	 * the memory pool of its type, increasing its available limit for the later allocations.
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
		if (segments != null) {
			segments.remove(segment);
		}
		releaseSegment(segment, releasedMemory);
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
			releaseSegment(segment, releasedMemory);
		}
		budgetByType.releaseBudgetForKeys(releasedMemory);

		segments.clear();
	}

	/**
	 * Reserves memory of a certain type for an owner from this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param memoryType type of memory to reserve (heap / off-heap).
	 * @param size size of memory to reserve.
	 * @throws MemoryAllocationException Thrown, if this memory manager does not have the requested amount
	 *                                   of memory any more.
	 */
	public void reserveMemory(Object owner, MemoryType memoryType, long size) throws MemoryAllocationException {
		checkMemoryReservationPreconditions(owner, memoryType, size);
		if (size == 0L) {
			return;
		}

		long acquiredMemory = budgetByType.acquireBudgetForKey(memoryType, size);
		if (acquiredMemory < size) {
			throw new MemoryAllocationException(
				String.format("Could not allocate %d bytes. Only %d bytes are remaining.", size, acquiredMemory));
		}

		reservedMemory.compute(owner, (o, reservations) -> {
			Map<MemoryType, Long> newReservations = reservations;
			if (reservations == null) {
				newReservations = new EnumMap<>(MemoryType.class);
				newReservations.put(memoryType, size);
			} else {
				reservations.compute(
					memoryType,
					(mt, currentlyReserved) -> currentlyReserved == null ? size : currentlyReserved + size);
			}
			return newReservations;
		});

		Preconditions.checkState(!isShutDown, "Memory manager has been concurrently shut down.");
	}

	/**
	 * Releases memory of a certain type from an owner to this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param memoryType type of memory to release (heap / off-heap).
	 * @param size size of memory to release.
	 */
	public void releaseMemory(Object owner, MemoryType memoryType, long size) {
		checkMemoryReservationPreconditions(owner, memoryType, size);
		if (size == 0L) {
			return;
		}

		reservedMemory.compute(owner, (o, reservations) -> {
			if (reservations != null) {
				reservations.compute(
					memoryType,
					(mt, currentlyReserved) ->
						currentlyReserved == null || currentlyReserved <= size ? null : currentlyReserved - size);
			}
			//noinspection ReturnOfNull
			return reservations == null || reservations.isEmpty() ? null : reservations;
		});
		budgetByType.releaseBudgetForKey(memoryType, size);
	}

	private void checkMemoryReservationPreconditions(Object owner, MemoryType memoryType, long size) {
		Preconditions.checkNotNull(owner, "The memory owner must not be null.");
		Preconditions.checkNotNull(memoryType, "The memory type must not be null.");
		Preconditions.checkState(!isShutDown, "Memory manager has been shut down.");
		Preconditions.checkArgument(size >= 0L, "The memory size (%s) has to have non-negative size", size);
	}

	/**
	 * Releases all memory of a certain type from an owner to this memory manager.
	 *
	 * @param owner The owner to associate with the memory reservation, for the fallback release.
	 * @param memoryType type of memory to release (heap / off-heap).
	 */
	public void releaseAllMemory(Object owner, MemoryType memoryType) {
		checkMemoryReservationPreconditions(owner, memoryType, 0L);

		reservedMemory.compute(owner, (o, reservations) -> {
			if (reservations != null) {
				Long size = reservations.remove(memoryType);
				if (size != null) {
					budgetByType.releaseBudgetForKey(memoryType, size);
				}
			}
			//noinspection ReturnOfNull
			return reservations == null || reservations.isEmpty() ? null : reservations;
		});
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
		return budgetByType.maxTotalBudget();
	}

	/**
	 * Returns the total size of the certain type of memory handled by this memory manager.
	 *
	 * @param memoryType The type of memory.
	 * @return The total size of memory.
	 */
	public long getMemorySizeByType(MemoryType memoryType) {
		return budgetByType.maxTotalBudgetForKey(memoryType);
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
		segment.free();
		releasedMemory.compute(getSegmentType(segment), (t, v) -> v == null ? pageSize : v + pageSize);
	}

	private static MemoryType getSegmentType(MemorySegment segment) {
		return segment.isOffHeap() ? MemoryType.OFF_HEAP : MemoryType.HEAP;
	}
}
