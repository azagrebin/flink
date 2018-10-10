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

package org.apache.flink.contrib.streaming.state.ttl;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.StateBackendTestContext;
import org.apache.flink.runtime.state.ttl.TtlStateTestBase;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDBException;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/** Base test suite for rocksdb state TTL. */
public abstract class RocksDBTtlStateTestBase extends TtlStateTestBase {
	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected StateBackendTestContext createStateBackendTestContext(TtlTimeProvider timeProvider) {
		return new StateBackendTestContext(timeProvider) {
			@Override
			protected StateBackend createStateBackend() {
				return RocksDBTtlStateTestBase.this.createStateBackend();
			}
		};
	}

	abstract StateBackend createStateBackend();

	StateBackend createStateBackend(TernaryBoolean enableIncrementalCheckpointing) {
		String dbPath;
		String checkpointPath;
		try {
			dbPath = tempFolder.newFolder().getAbsolutePath();
			checkpointPath = tempFolder.newFolder().toURI().toString();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to init rocksdb test state backend");
		}
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), enableIncrementalCheckpointing);
		backend.setDbStoragePath(dbPath);
		return backend;
	}

	@Test
	public void testCompactFilter() throws Exception {
		testCompactFilter(false);
	}

	@Test
	public void testCompactFilterWithSnapshot() throws Exception {
		testCompactFilter(true);
	}

	@SuppressWarnings("resource")
	private void testCompactFilter(boolean takeSnapshot) throws Exception {
		StateDescriptor<?, ?> stateDesc = initTest(getConfBuilder(TTL).cleanupInRocksdbCompactFilter().setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp).build());

		setTimeAndCompact(stateDesc, 0L);

		sbetc.setCurrentKey("k1");
		ctx().update(ctx().updateEmpty);
		checkUnexpiredOriginalAvailable();

		sbetc.setCurrentKey("k2");
		ctx().update(ctx().updateEmpty);
		checkUnexpiredOriginalAvailable();

		if (takeSnapshot) {
			takeAndRestoreSnapshot();
		}

		setTimeAndCompact(stateDesc, 50L);

		sbetc.setCurrentKey("k1");
		checkUnexpiredOriginalAvailable();
		assertEquals("Unexpired state should be available", ctx().getUpdateEmpty, ctx().get());

		ctx().update(ctx().updateUnexpired);
		checkUnexpiredOriginalAvailable();

		sbetc.setCurrentKey("k2");
		checkUnexpiredOriginalAvailable();
		assertEquals("Unexpired state should be available", ctx().getUpdateEmpty, ctx().get());

		ctx().update(ctx().updateUnexpired);
		checkUnexpiredOriginalAvailable();

		if (takeSnapshot) {
			takeAndRestoreSnapshot();
		}

		//setTimeAndCompact(stateDesc, 80L); // TODO: test merged elements expiration
		setTimeAndCompact(stateDesc, 120L);

		sbetc.setCurrentKey("k1");
		checkUnexpiredOriginalAvailable();
		assertEquals("Unexpired state should be available after update", ctx().getUnexpired, ctx().get());

		sbetc.setCurrentKey("k2");
		checkUnexpiredOriginalAvailable();
		assertEquals("Unexpired state should be available after update", ctx().getUnexpired, ctx().get());

		if (takeSnapshot) {
			takeAndRestoreSnapshot();
		}

		setTimeAndCompact(stateDesc, 170L);
		sbetc.setCurrentKey("k1");
		assertEquals("Expired original state should be unavailable", ctx().emptyValue, ctx().getOriginal());
		assertEquals("Expired state should be unavailable", ctx().emptyValue, ctx().get());

		sbetc.setCurrentKey("k2");
		assertEquals("Expired original state should be unavailable", ctx().emptyValue, ctx().getOriginal());
		assertEquals("Expired state should be unavailable", ctx().emptyValue, ctx().get());
	}

	private void checkUnexpiredOriginalAvailable() throws Exception {
		assertNotEquals("Unexpired original state should be available", ctx().emptyValue, ctx().getOriginal());
	}

	private void setTimeAndCompact(StateDescriptor<?, ?> stateDesc, long ts) throws RocksDBException {
		@SuppressWarnings("resource")
		RocksDBKeyedStateBackend<String> keyedBackend = sbetc.getKeyedStateBackend();
		timeProvider.time = ts;
		keyedBackend.setCompactFilterTime(stateDesc, ts);
		keyedBackend.compactRangeForKvState(ctx().getName());
	}
}
