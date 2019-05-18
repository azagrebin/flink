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

package org.apache.flink.runtime.io.network.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.metrics.MetricNames;

/**
 * Collects metrics for {@link RemoteInputChannel} and {@link LocalInputChannel}.
 */
public class InputChannelMetrics {

	private static final String IO_NUM_BYTES_IN_LOCAL = MetricNames.IO_NUM_BYTES_IN + "Local";
	private static final String IO_NUM_BYTES_IN_REMOTE = MetricNames.IO_NUM_BYTES_IN + "Remote";
	private static final String IO_NUM_BUFFERS_IN_LOCAL = MetricNames.IO_NUM_BUFFERS_IN + "Local";
	private static final String IO_NUM_BUFFERS_IN_REMOTE = MetricNames.IO_NUM_BUFFERS_IN + "Remote";

	private final Counter numBytesInLocal;
	private final Counter numBytesInRemote;
	private final Counter numBuffersInLocal;
	private final Counter numBuffersInRemote;

	public InputChannelMetrics(MetricGroup parent) {
		this.numBytesInLocal = createCounter(parent, IO_NUM_BYTES_IN_LOCAL);
		this.numBytesInRemote = createCounter(parent, IO_NUM_BYTES_IN_REMOTE);
		this.numBuffersInLocal = createCounter(parent, IO_NUM_BUFFERS_IN_LOCAL);
		this.numBuffersInRemote = createCounter(parent, IO_NUM_BUFFERS_IN_REMOTE);
	}

	private static Counter createCounter(MetricGroup parent, String name) {
		Counter counter = parent.counter(name);
		parent.meter(name + MetricNames.SUFFIX_RATE, new MeterView(counter, 60));
		return counter;
	}

	public void incNumBytesInLocalCounter(long inc) {
		numBytesInLocal.inc(inc);
	}

	public void incNumBytesInRemoteCounter(long inc) {
		numBytesInRemote.inc(inc);
	}

	public void incNumBuffersInLocalCounter(long inc) {
		numBuffersInLocal.inc(inc);
	}

	public void incNumBuffersInRemoteCounter(long inc) {
		numBuffersInRemote.inc(inc);
	}

	/**
	 * Wraps {@link InputChannelMetrics} with legacy metrics.
	 * @deprecated eventually should be removed in favour of normal {@link InputChannelMetrics}.
	 */
	@SuppressWarnings("DeprecatedIsStillUsed")
	@Deprecated
	public static class InputChannelMetricsWithLegacy extends InputChannelMetrics {
		private final InputChannelMetrics legacyMetrics;

		public InputChannelMetricsWithLegacy(MetricGroup parent, MetricGroup legacyParent) {
			super(parent);
			legacyMetrics = new InputChannelMetrics(legacyParent);
		}

		@Override
		public void incNumBytesInLocalCounter(long inc) {
			super.incNumBytesInLocalCounter(inc);
			legacyMetrics.incNumBytesInLocalCounter(inc);
		}

		@Override
		public void incNumBytesInRemoteCounter(long inc) {
			super.incNumBytesInRemoteCounter(inc);
			legacyMetrics.incNumBytesInRemoteCounter(inc);
		}

		@Override
		public void incNumBuffersInLocalCounter(long inc) {
			super.incNumBuffersInLocalCounter(inc);
			legacyMetrics.incNumBuffersInLocalCounter(inc);
		}

		@Override
		public void incNumBuffersInRemoteCounter(long inc) {
			super.incNumBuffersInRemoteCounter(inc);
			legacyMetrics.incNumBuffersInRemoteCounter(inc);
		}
	}
}
