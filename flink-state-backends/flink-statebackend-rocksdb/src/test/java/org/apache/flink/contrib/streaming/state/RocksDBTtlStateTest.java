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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.StateBackendTestContext;
import org.apache.flink.runtime.state.ttl.TtlListStateTestContext;
import org.apache.flink.runtime.state.ttl.TtlMapStateTestContext;
import org.apache.flink.runtime.state.ttl.TtlStateTestBase;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TernaryBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlinkCompactionFilter;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** Test suite for rocksdb state TTL. */
public class RocksDBTtlStateTest extends TtlStateTestBase {
	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected StateBackendTestContext createStateBackendTestContext(TtlTimeProvider timeProvider) {
		return new StateBackendTestContext(timeProvider) {
			@Override
			protected StateBackend createStateBackend() {
				return RocksDBTtlStateTest.this.createStateBackend();
			}
		};
	}

	private StateBackend createStateBackend() {
		String dbPath;
		String checkpointPath;
		try {
			dbPath = tempFolder.newFolder().getAbsolutePath();
			checkpointPath = tempFolder.newFolder().toURI().toString();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to init rocksdb test state backend");
		}
		RocksDBStateBackend backend = new RocksDBStateBackend(new FsStateBackend(checkpointPath), TernaryBoolean.FALSE);
		backend.setDbStoragePath(dbPath);
		backend.setOptions(new OptionsFactory() {
			private static final long serialVersionUID = -3791222437886983105L;

			@Override
			public DBOptions createDBOptions(DBOptions currentOptions) {
				return currentOptions;
			}

			@Override
			public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
				return currentOptions.setCompactionFilter(
					new FlinkCompactionFilter(
						getStateType(),
						ttlConfig.getTtl().toMilliseconds(),
						timeProvider::currentTimestamp));
			}
		});
		return backend;
	}

	private FlinkCompactionFilter.StateType getStateType() {
		if (ctx instanceof TtlListStateTestContext) {
			return FlinkCompactionFilter.StateType.List;
		} else if (ctx instanceof TtlMapStateTestContext) {
			return FlinkCompactionFilter.StateType.Map;
		} else {
			return FlinkCompactionFilter.StateType.Value;
		}
	}

	@Test
	public void testCompactFilter() throws Exception {
		initTest();
		//noinspection resource
		RocksDBKeyedStateBackend<String> keyedBackend = sbetc.getKeyedStateBackend();

		timeProvider.time = 0;
		sbetc.setCurrentKey("k1_" + ctx.ttlState.getClass().getSimpleName());
		ctx().update(ctx().updateEmpty);
		sbetc.setCurrentKey("k2_" + ctx.ttlState.getClass().getSimpleName());
		ctx().update(ctx().updateEmpty);

		timeProvider.time = 120;
		ColumnFamilyHandle cfh = keyedBackend.kvStateInformation.values().iterator().next().f0;
		keyedBackend.db.compactRange(cfh);
		sbetc.setCurrentKey("k1_" + ctx.ttlState.getClass().getSimpleName());
		assertEquals("Expired original state should be unavailable", ctx().emptyValue, ctx().getOriginal());
		sbetc.setCurrentKey("k2_" + ctx.ttlState.getClass().getSimpleName());
		assertEquals("Expired original state should be unavailable", ctx().emptyValue, ctx().getOriginal());
	}
}
