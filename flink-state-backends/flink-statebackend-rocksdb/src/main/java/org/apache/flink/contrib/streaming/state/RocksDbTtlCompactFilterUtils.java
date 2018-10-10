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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.FlinkCompactionFilter;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnull;

import java.io.IOException;

class RocksDbTtlCompactFilterUtils {
	static FlinkCompactionFilter setCompactFilterIfStateTtl(
		RegisteredStateMetaInfoBase metaInfoBase, ColumnFamilyOptions options) {
		if (metaInfoBase instanceof RegisteredKeyValueStateBackendMetaInfo) {
			RegisteredKeyValueStateBackendMetaInfo kvMetaInfoBase = (RegisteredKeyValueStateBackendMetaInfo) metaInfoBase;
			if (TtlStateFactory.TtlSerializer.isTtlStateSerializer(kvMetaInfoBase.getStateSerializer())) {
				FlinkCompactionFilter compactFilter = new FlinkCompactionFilter(InfoLogLevel.ERROR_LEVEL);
				//noinspection resource
				options.setCompactionFilter(compactFilter);
				return compactFilter;
			}
		}
		return null;
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
			FlinkCompactionFilter.StateType type;
			int timestampOffset = 0;
			if (stateDesc instanceof ListStateDescriptor) {
				type = FlinkCompactionFilter.StateType.List;
			} else if (stateDesc instanceof MapStateDescriptor) {
				type = FlinkCompactionFilter.StateType.Map;
				timestampOffset = 1;
			} else {
				type = FlinkCompactionFilter.StateType.Value;
			}
			stateInfo.compactionFilter.configure(type, timestampOffset, ttlConfig.getTtl().toMilliseconds(), useSystemTime);
		}
	}

	static RocksDBKeyedStateBackend.StateRestoreWriter createStateRestoreWriter(
		@Nonnull RocksDBWriteBatchWrapper writeBatchWrapper,
		@Nonnull RocksDbKvStateInfo kvStateInfo) {
		if (kvStateInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo) {
			RegisteredKeyValueStateBackendMetaInfo kvMetaInfoBase =
				(RegisteredKeyValueStateBackendMetaInfo) kvStateInfo.metaInfo;
			if (kvMetaInfoBase.getStateType() == StateDescriptor.Type.LIST &&
				TtlStateFactory.TtlSerializer.isTtlStateSerializer(kvMetaInfoBase.getStateSerializer())) {
				return new ListMergeStateRestoreWriter(writeBatchWrapper, kvStateInfo);
			}
		}
		return (key, value) -> writeBatchWrapper.put(kvStateInfo.columnFamilyHandle, key, value);
	}

	private static class ListMergeStateRestoreWriter implements RocksDBKeyedStateBackend.StateRestoreWriter {
		@Nonnull private final DataInputDeserializer dataInputView;
		@Nonnull private final DataOutputSerializer dataOutputView;
		@Nonnull private final RocksDBWriteBatchWrapper writeBatchWrapper;
		@Nonnull private final RocksDbKvStateInfo kvStateInfo;
		@Nonnull private final TypeSerializer<?> elementSerializer;

		private ListMergeStateRestoreWriter(
				@Nonnull RocksDBWriteBatchWrapper writeBatchWrapper,
				@Nonnull RocksDbKvStateInfo kvStateInfo) {
			this.dataInputView = new DataInputDeserializer();
			this.dataOutputView = new DataOutputSerializer(128);
			this.writeBatchWrapper = writeBatchWrapper;
			this.kvStateInfo = kvStateInfo;
			this.elementSerializer =
				((ListSerializer<?>) ((RegisteredKeyValueStateBackendMetaInfo) kvStateInfo.metaInfo)
					.getStateSerializer()).getElementSerializer();
		}

		@Override
		public void restore(@Nonnull byte[] key, @Nonnull byte[] value) throws RocksDBException, IOException {
			dataInputView.setBuffer(value);
			byte[] element;
			while ((element = nextElement(dataInputView, dataOutputView, elementSerializer)) != null) {
				writeBatchWrapper.merge(kvStateInfo.columnFamilyHandle, key, element);
			}
		}
	}

	private static <T> byte[] nextElement(
		DataInputDeserializer dataInputView,
		DataOutputSerializer dataOutputView,
		TypeSerializer<T> elementSerializer) throws IOException {
		dataOutputView.clear();
		T value = RocksDBListState.deserializeNextElement(dataInputView, elementSerializer);
		if (value != null) {
			elementSerializer.serialize(value, dataOutputView);
			return dataOutputView.getCopyOfBuffer();
		} else {
			return null;
		}
	}
}
