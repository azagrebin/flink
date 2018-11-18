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

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlinkCompactionFilter;
import org.rocksdb.InfoLogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class RocksDbTtlCompactFilterUtils {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkCompactionFilter.class);

	static FlinkCompactionFilter setCompactFilterIfStateTtl(
		RegisteredStateMetaInfoBase metaInfoBase, ColumnFamilyOptions options) {
		if (metaInfoBase instanceof RegisteredKeyValueStateBackendMetaInfo) {
			RegisteredKeyValueStateBackendMetaInfo kvMetaInfoBase = (RegisteredKeyValueStateBackendMetaInfo) metaInfoBase;
			if (TtlStateFactory.TtlSerializer.isTtlStateSerializer(kvMetaInfoBase.getStateSerializer())) {
				FlinkCompactionFilter compactFilter = new FlinkCompactionFilter(createLogger());
				//noinspection resource
				options.setCompactionFilter(compactFilter);
				return compactFilter;
			}
		}
		return null;
	}

	private static org.rocksdb.Logger createLogger() {
		//if (LOG.isDebugEnabled()) {
			try (DBOptions opts = new DBOptions().setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)) {
				return new org.rocksdb.Logger(opts) {
					@Override
					protected void log(InfoLogLevel infoLogLevel, String logMsg) {
						System.out.println("Native code log: " + logMsg);
					}
				};
			}
//		} else {
//			return null;
//		}
	}

	static void configCompactFilter(StateDescriptor<?, ?> stateDesc, RocksDbKvStateInfo stateInfo) {
		StateTtlConfig ttlConfig = stateDesc.getTtlConfig();
		configCompactFilter(stateDesc, stateInfo,
			ttlConfig.getTimeCharacteristic() == StateTtlConfig.TimeCharacteristic.ProcessingTime);
	}

	static void configCompactFilter(StateDescriptor<?, ?> stateDesc, RocksDbKvStateInfo stateInfo, boolean useSystemTime) {
		StateTtlConfig ttlConfig = stateDesc.getTtlConfig();
		if (ttlConfig.isEnabled() && ttlConfig.getCleanupStrategies().inRocksdbCompactFilter()) {
			assert stateInfo.compactionFilter != null;
			FlinkCompactionFilter.Config config;
			if (stateDesc instanceof ListStateDescriptor) {
				ListStateDescriptor<?> listStateDesc = (ListStateDescriptor<?>) stateDesc;
				int len = listStateDesc.getElementSerializer().getLength();
				if (len > 0) {
					config = FlinkCompactionFilter.Config.createForFixedElementList(
						0, ttlConfig.getTtl().toMilliseconds(), useSystemTime, len + 1);
				} else {
					config = FlinkCompactionFilter.Config.createForList(
						0, ttlConfig.getTtl().toMilliseconds(), useSystemTime,
						new ListElementIter<>(listStateDesc.getElementSerializer()));
				}
			} else if (stateDesc instanceof MapStateDescriptor) {
				config = FlinkCompactionFilter.Config.create(
					FlinkCompactionFilter.StateType.Map, 1, ttlConfig.getTtl().toMilliseconds(), useSystemTime);
			} else {
				config = FlinkCompactionFilter.Config.create(
					FlinkCompactionFilter.StateType.Value, 0, ttlConfig.getTtl().toMilliseconds(), useSystemTime);
			}
			stateInfo.compactionFilter.configure(config);
		}
	}

	private static class ListElementIter<T> implements FlinkCompactionFilter.ListElementIter {
		private final TypeSerializer<T> serializer;
		private DataInputDeserializer input;

		private ListElementIter(TypeSerializer<T> serializer) {
			this.serializer = serializer;
		}

		@Override
		public void setListBytes(byte[] bytes) {
			input = new DataInputDeserializer(bytes);
		}

		@Override
		public int nextOffset(int currentOffset) {
			try {
				serializer.deserialize(input);
				if (input.available() > 0) {
					input.skipBytesToRead(1);
				}
			} catch (IOException e) {
				throw new FlinkRuntimeException("Failed to deserialize list element for TTL compaction filter", e);
			}
			return input.getPosition();
		}
	}
}
