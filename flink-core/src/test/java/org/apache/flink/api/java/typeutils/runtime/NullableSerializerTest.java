package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link NullableSerializer}. */
public class NullableSerializerTest extends SerializerTestBase<Integer> {
	private static final TypeSerializer<Integer> originalSerializer = IntSerializer.INSTANCE;
	private static final TypeSerializer<Integer> nullableSerializer =
		NullableSerializer.wrapIfNullIsNotSupported(originalSerializer);

	@Override
	protected TypeSerializer<Integer> createSerializer() {
		return NullableSerializer.wrapIfNullIsNotSupported(originalSerializer);
	}

	@Override
	protected int getLength() {
		return 5;
	}

	@Override
	protected Class<Integer> getTypeClass() {
		return Integer.class;
	}

	@Override
	protected Integer[] getTestData() {
		return new Integer[] { 5, -1, 0, null };
	}

	@Test
	public void testWrappingNotNeeded() {
		assertEquals(NullableSerializer.wrapIfNullIsNotSupported(StringSerializer.INSTANCE), StringSerializer.INSTANCE);
	}

	@Test
	public void testWrappingNeeded() {
		assertTrue(nullableSerializer instanceof NullableSerializer);
		assertEquals(NullableSerializer.wrapIfNullIsNotSupported(nullableSerializer), nullableSerializer);
	}
}
