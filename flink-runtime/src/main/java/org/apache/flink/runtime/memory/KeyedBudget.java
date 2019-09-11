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

import javax.annotation.concurrent.GuardedBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

class KeyedBudget<K> {
	private final Map<K, Long> maxBudgetByKey;

	@GuardedBy("lock")
	private final Map<K, Long> availableBudgetByKey;

	private final Object lock = new Object();

	KeyedBudget(Map<K, Long> maxBudgetByKey) {
		this.maxBudgetByKey = new HashMap<>(maxBudgetByKey);
		this.availableBudgetByKey = new HashMap<>(maxBudgetByKey);
	}

	/**
	 * Tries to acquire budget which equals to the number of pages times page size.
	 *
	 * <p>See also {@link #acquirePagedBudgetForKeys(Iterable, long, long)}
	 */
	Either<Map<K, Long>, Long> acquirePagedBudget(int pageNum, long pageSize) {
		return acquirePagedBudgetForKeys(maxBudgetByKey.keySet(), pageNum, pageSize);
	}

	/**
	 * Tries to acquire budget for a given key.
	 *
	 * <p>No budget is acquired if it was not possible to fully acquire the requested budget.
	 *
	 * @param key the key to acquire budget from
	 * @param size the size of budget to acquire from the given key
	 * @return the fully acquired budget for the key or max possible budget to acquir
	 * if it was not possible to acquire the requested budget.
	 */
	long acquireBudgetForKey(K key, long size) {
		Either<Map<K, Long>, Long> result = acquirePagedBudgetForKeys(Collections.singletonList(key), size, 1L);
		return result.isLeft() ? result.left().get(key) : result.right();
	}

	/**
	 * Tries to acquire budget which equals to the number of pages times page size.
	 *
	 * <p>The budget will be acquired only from the given keys. Only integer number of pages will be acquired from each key.
	 * If the next page does not fit into the available budget of some key, it will try to be acquired from another key.
	 * The acquisition is successful if the acquired number of pages for each key sums up to the requested number of pages.
	 * The function does not make any preference about which keys from the given keys to acquire from.
	 *
	 * @param keys the keys to acquire budget from
	 * @param pageNum the total number of pages to acquire from the given keys
	 * @param pageSize the size of budget to acquire per page
	 * @return the acquired number of pages for each key if the acquisition is successful (either left) or
	 * the total number of pages which were available for the given keys (either right).
	 */
	Either<Map<K, Long>, Long> acquirePagedBudgetForKeys(Iterable<K> keys, long pageNum, long pageSize) {
		synchronized (lock) {
			long totalPossiblePages = 0L;
			Map<K, Long> pagesToReserveByKey = new HashMap<>();
			for (K key : keys) {
				long currentKeyBudget = availableBudgetByKey.getOrDefault(key, 0L);
				long currentKeyPages = currentKeyBudget / pageSize;
				if (totalPossiblePages + currentKeyPages >= pageNum) {
					pagesToReserveByKey.put(key, pageNum - totalPossiblePages);
					totalPossiblePages = pageNum;
				} else if (currentKeyPages > 0L) {
					pagesToReserveByKey.put(key, currentKeyPages);
					totalPossiblePages += currentKeyPages;
				}
			}
			boolean possibleToAcquire = totalPossiblePages == pageNum;
			if (possibleToAcquire) {
				for (Entry<K, Long> pagesToReserveForKey : pagesToReserveByKey.entrySet()) {
					//noinspection ConstantConditions
					availableBudgetByKey.compute(
						pagesToReserveForKey.getKey(),
						(k, v) -> v - (pagesToReserveForKey.getValue() * pageSize));
				}
			}
			return possibleToAcquire ? Either.Left(pagesToReserveByKey) : Either.Right(totalPossiblePages);
		}
	}

	void releaseBudgetForKey(K key, long size) {
		releaseBudgetForKeys(Collections.singletonMap(key, size));
	}

	void releaseBudgetForKeys(Map<K, Long> sizeByKey) {
		synchronized (lock) {
			for (Entry<K, Long> toReleaseForKey : sizeByKey.entrySet()) {
				long toRelease = toReleaseForKey.getValue();
				if (toRelease == 0L) {
					continue;
				}
				K keyToReleaseFor = toReleaseForKey.getKey();
				long maxBudgetForKey = maxBudgetByKey.get(keyToReleaseFor);
				availableBudgetByKey.compute(keyToReleaseFor, (k, currentBudget) -> {
					if (currentBudget == null) {
						throw new IllegalArgumentException("The budget key is not supported: " + keyToReleaseFor);
					} else if (currentBudget + toRelease > maxBudgetForKey) {
						throw new IllegalStateException(
							String.format(
								"The budget to release %d exceeds the limit %d for key %s",
								toRelease,
								maxBudgetForKey,
								keyToReleaseFor));
					} else {
						return currentBudget + toRelease;
					}
				});
			}
		}
	}

	void releaseAll() {
		synchronized (lock) {
			availableBudgetByKey.putAll(maxBudgetByKey);
		}
	}

	long maxTotalBudget() {
		return maxBudgetByKey.values().stream().mapToLong(b -> b).sum();
	}

	long maxTotalBudgetForKey(K key) {
		return maxBudgetByKey.get(key);
	}

	long totalAvailableBudget() {
		return availableBudgetForKeys(maxBudgetByKey.keySet());
	}

	long availableBudgetForKeys(Iterable<K> keys) {
		synchronized (lock) {
			long totalSize = 0L;
			for (K key : keys) {
				totalSize += availableBudgetForKey(key);
			}
			return totalSize;
		}
	}

	long availableBudgetForKey(K key) {
		synchronized (lock) {
			return availableBudgetByKey.getOrDefault(key, 0L);
		}
	}
}
