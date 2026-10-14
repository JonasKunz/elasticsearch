/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/** Tests for dictionary-encoded STRING columns: building, reading, slicing, plain fallback, scattering and the wire format. */
public class EscfDictStringColumnTests extends ESTestCase {

    private static EscfColumnBuilder dictBuilder() {
        return new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, BytesRefRecycler.NON_RECYCLING_INSTANCE, true);
    }

    private static void addString(EscfColumnBuilder builder, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        builder.addString(new XContentString.UTF8Bytes(bytes, 0, bytes.length));
    }

    /** Builds a column from {@code values}, where {@code null} is an absent row. */
    private static EscfColumn build(List<String> values) {
        EscfColumnBuilder builder = dictBuilder();
        for (String value : values) {
            if (value == null) {
                builder.addAbsent();
            } else {
                addString(builder, value);
            }
        }
        return EscfColumn.from(builder.finish(values.size()));
    }

    private static void assertValues(List<String> expected, EscfColumn column) {
        assertEquals(EscfColumnKind.STRING, column.kind());
        assertEquals(expected.size(), column.docCount());
        for (int row = 0; row < expected.size(); row++) {
            String value = expected.get(row);
            if (value == null) {
                assertTrue("row " + row, column.getTypeByte(row) == SourceValueType.ABSENT);
            } else {
                assertEquals("row " + row, SourceValueType.STRING, column.getTypeByte(row));
                assertEquals("row " + row, value, column.getBinaryValue(row).utf8ToString());
                assertEquals("row " + row, value, column.getStringValue(row).string());
            }
        }
        for (boolean retain : new boolean[] { false, true }) {
            ObjectTupleCursor<BytesRef> cursor = column.bytesRefCursor(retain);
            List<String> seen = new ArrayList<>();
            List<BytesRef> retained = new ArrayList<>();
            for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                assertNotNull("doc " + doc + " should be present", expected.get(doc));
                seen.add(doc + "=" + cursor.value().utf8ToString());
                retained.add(cursor.value());
            }
            List<String> expectedSeen = new ArrayList<>();
            for (int doc = 0; doc < expected.size(); doc++) {
                if (expected.get(doc) != null) {
                    expectedSeen.add(doc + "=" + expected.get(doc));
                }
            }
            assertEquals("retain=" + retain, expectedSeen, seen);
            if (retain) {
                for (int i = 0; i < retained.size(); i++) {
                    assertEquals(
                        "retained value " + i,
                        expectedSeen.get(i).substring(expectedSeen.get(i).indexOf('=') + 1),
                        retained.get(i).utf8ToString()
                    );
                }
            }
        }
    }

    private static List<String> randomValues(int rows, int distinct, boolean withAbsents) {
        List<String> values = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            if (withAbsents && randomInt(4) == 0) {
                values.add(null);
            } else {
                values.add(
                    randomBoolean() ? "v" + randomInt(distinct - 1) : randomAlphaOfLength(randomIntBetween(0, 12)) + randomInt(distinct - 1)
                );
            }
        }
        if (values.stream().allMatch(v -> v == null)) {
            values.set(0, "present"); // an all-absent column has no kind, so it would not be a STRING column at all
        }
        return values;
    }

    public void testBuildsDictionaryColumn() {
        List<String> values = List.of("a", "b", "a", "a", "b", "", "a");
        EscfColumn column = build(values);
        assertThat(column, instanceOf(EscfDictStringColumn.class));
        assertTrue(column.isDense());
        assertValues(values, column);
    }

    public void testAbsentRows() {
        List<String> values = new ArrayList<>();
        values.add(null);
        values.add(null);
        values.add("x");
        values.add(null);
        values.add("y");
        values.add("x");
        values.add(null);
        EscfColumn column = build(values);
        assertThat(column, instanceOf(EscfDictStringColumn.class));
        assertFalse(column.isDense());
        assertValues(values, column);
    }

    public void testAllAbsent() {
        List<String> values = new ArrayList<>();
        values.add(null);
        values.add(null);
        // an all-absent column never learns its kind, so it is not a dictionary column; nothing to assert beyond not crashing
        EscfColumnBuilder builder = dictBuilder();
        builder.hintScalar(EscfColumnKind.STRING);
        builder.addAbsents(2);
        EscfColumn column = EscfColumn.from(builder.finish(2));
        assertThat(column, instanceOf(EscfDictStringColumn.class));
        assertValues(values, column);
    }

    public void testRandomized() {
        for (int iter = 0; iter < 20; iter++) {
            List<String> values = randomValues(randomIntBetween(1, 300), randomIntBetween(1, 40), randomBoolean());
            EscfColumn column = build(values);
            assertValues(values, column);
        }
    }

    public void testDictionaryStoresEachValueOnce() {
        EscfColumnBuilder builder = dictBuilder();
        String value = "x".repeat(100);
        for (int i = 0; i < 1000; i++) {
            addString(builder, value);
        }
        EscfColumnData data = builder.finish(1000);
        assertTrue(data.isDictionaryEncoded());
        assertEquals(2, data.offsets().length);
        assertEquals(100, data.data().length());
        assertEquals(1000, data.ordinals().length);
    }

    public void testSwitchesToPlainBeyondMaxDictionarySize() {
        int distinct = EscfColumnData.MAX_DICTIONARY_SIZE + 10;
        List<String> values = new ArrayList<>();
        values.add(null);
        for (int i = 0; i < distinct; i++) {
            values.add("value-" + i);
            if (i % 1000 == 0) {
                values.add(null);
            }
            if (i % 777 == 0) {
                values.add("value-0");
            }
        }
        EscfColumn column = build(values);
        assertThat(column, instanceOf(EscfStringColumn.class));
        assertValues(values, column);
    }

    public void testExactlyMaxDictionarySizeStaysDictionary() {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < EscfColumnData.MAX_DICTIONARY_SIZE; i++) {
            values.add("value-" + i);
        }
        values.add("value-0");
        EscfColumn column = build(values);
        assertThat(column, instanceOf(EscfDictStringColumn.class));
        assertValues(values, column);
    }

    public void testPromotesToUnionWhenAnotherKindArrives() {
        EscfColumnBuilder builder = dictBuilder();
        addString(builder, "a");
        builder.addAbsent();
        addString(builder, "a");
        builder.addLong(42);
        addString(builder, "b");
        EscfColumn column = EscfColumn.from(builder.finish(5));
        assertEquals(EscfColumnKind.UNION, column.kind());
        assertEquals("a", column.getStringValue(0).string());
        assertEquals(SourceValueType.ABSENT, column.getTypeByte(1));
        assertEquals("a", column.getStringValue(2).string());
        assertEquals(42, column.getLongValue(3));
        assertEquals("b", column.getStringValue(4).string());
    }

    public void testSlice() {
        List<String> values = randomValues(200, 10, true);
        EscfColumn column = build(values);
        assertThat(column, instanceOf(EscfDictStringColumn.class));
        for (int iter = 0; iter < 20; iter++) {
            int from = randomIntBetween(0, values.size() - 1);
            int count = randomIntBetween(1, values.size() - from);
            EscfColumn slice = (EscfColumn) column.slice(from, count);
            assertThat(slice, instanceOf(EscfDictStringColumn.class));
            assertValues(values.subList(from, from + count), slice);

            // a slice survives the native round trip
            EscfColumn rebuilt = EscfColumn.from(slice.toColumnData());
            assertValues(values.subList(from, from + count), rebuilt);
        }
    }

    public void testColumnDataIsPlain() {
        List<String> values = randomValues(100, 8, true);
        EscfColumn column = build(values);
        assertThat(column, instanceOf(EscfDictStringColumn.class));

        EscfColumnData plain = column.columnData();
        assertFalse(plain.isDictionaryEncoded());
        assertEquals(values.size() + 1, plain.offsets().length);
        assertSame("the expansion is cached", plain, column.columnData());
        EscfColumn plainColumn = EscfColumn.from(plain);
        assertThat(plainColumn, instanceOf(EscfStringColumn.class));
        assertValues(values, plainColumn);

        // and the native form keeps the dictionary
        assertTrue(column.toColumnData().isDictionaryEncoded());
    }

    public void testLuceneBinaryColumnOverDictionaryColumn() {
        List<String> values = List.of("a", "b", "a", "c");
        EscfColumn column = build(values);
        // mappers hand columnData() to LuceneBinaryColumn, which needs the plain layout
        LuceneBinaryColumn lucene = LuceneBinaryColumn.of(column.columnData(), "f", org.apache.lucene.document.StringField.TYPE_NOT_STORED);
        assertNotNull(lucene);
    }

    public void testWireRoundTrip() throws Exception {
        for (int iter = 0; iter < 10; iter++) {
            int rows = randomIntBetween(1, 200);
            List<String> resource = randomValues(rows, 3, false);
            List<String> attr = randomValues(rows, 30, true);
            attr.set(0, "always"); // an all-absent column has no kind to encode; row 0 also pins the schema order
            try (EscfBatchBuilder builder = new EscfBatchBuilder(BytesRefRecycler.NON_RECYCLING_INSTANCE, true)) {
                for (int r = 0; r < rows; r++) {
                    builder.beginRow();
                    byte[] resourceBytes = resource.get(r).getBytes(StandardCharsets.UTF_8);
                    builder.stringField("resource", resourceBytes, 0, resourceBytes.length);
                    if (attr.get(r) != null) {
                        byte[] bytes = attr.get(r).getBytes(StandardCharsets.UTF_8);
                        builder.stringField("attr", bytes, 0, bytes.length);
                    }
                    builder.longField("n", r);
                    builder.finishRow();
                }
                try (EscfBatch batch = builder.build()) {
                    assertThat(batch.column(0), instanceOf(EscfDictStringColumn.class));
                    assertThat(batch.column(1), instanceOf(EscfDictStringColumn.class));
                    BytesReference bytes = batch.data();
                    try (EscfBatch parsed = EscfBatch.parse(bytes, () -> {})) {
                        assertThat(parsed.column(0), instanceOf(EscfDictStringColumn.class));
                        assertThat(parsed.column(1), instanceOf(EscfDictStringColumn.class));
                        assertValues(resource, parsed.column(0));
                        assertValues(attr, parsed.column(1));
                        assertEquals(bytes, parsed.data());
                        assertEquals(batch.estimatedBytes(), parsed.estimatedBytes());

                        int from = randomIntBetween(0, rows - 1);
                        int to = randomIntBetween(from + 1, rows);
                        try (EscfBatch resliced = EscfBatch.parse(parsed.slice(from, to).data(), () -> {})) {
                            assertValues(resource.subList(from, to), resliced.column(0));
                            assertValues(attr.subList(from, to), resliced.column(1));
                        }
                    }
                }
            }
        }
    }

    public void testSerializedSizeSmallerThanPlain() {
        int rows = 2000;
        BytesReference dict = serializeRepeatedValues(rows, true);
        BytesReference plain = serializeRepeatedValues(rows, false);
        assertThat(dict.length(), lessThan(plain.length() / 2));
    }

    private static BytesReference serializeRepeatedValues(int rows, boolean dictionary) {
        try (EscfBatchBuilder builder = new EscfBatchBuilder(BytesRefRecycler.NON_RECYCLING_INSTANCE, dictionary)) {
            for (int r = 0; r < rows; r++) {
                builder.beginRow();
                byte[] bytes = ("host-" + (r % 4) + ".example.internal").getBytes(StandardCharsets.UTF_8);
                builder.stringField("host", bytes, 0, bytes.length);
                builder.finishRow();
            }
            try (EscfBatch batch = builder.build()) {
                return batch.data();
            }
        }
    }

    public void testBatchRamAndEstimatedBytesCountOrdinals() {
        try (EscfBatchBuilder builder = new EscfBatchBuilder(BytesRefRecycler.NON_RECYCLING_INSTANCE, true)) {
            for (int r = 0; r < 100; r++) {
                builder.beginRow();
                builder.stringField("f", new byte[] { 'a' }, 0, 1);
                builder.finishRow();
            }
            try (EscfBatch batch = builder.build()) {
                // 1 dictionary byte + 100 two-byte ordinals + 2 dictionary offsets of 4 bytes + the 64-byte fixed overhead
                assertEquals(64 + 8 + 1 + 200, batch.ramBytesUsed());
                assertTrue(batch.estimatedBytes() >= 201);
            }
        }
    }

    public void testScatterKeepsDictionaryEncoding() {
        int rows = 60;
        List<String> values = randomValues(rows, 5, true);
        values.set(0, "first"); // row 0 pins "f" as the first schema column
        int partitions = 3;
        int[] partitionIds = new int[rows];
        for (int r = 0; r < rows; r++) {
            partitionIds[r] = randomInt(partitions - 1);
        }
        try (EscfBatchBuilder builder = new EscfBatchBuilder(BytesRefRecycler.NON_RECYCLING_INSTANCE, true)) {
            for (int r = 0; r < rows; r++) {
                builder.beginRow();
                if (values.get(r) != null) {
                    byte[] bytes = values.get(r).getBytes(StandardCharsets.UTF_8);
                    builder.stringField("f", bytes, 0, bytes.length);
                } else {
                    builder.longField("other", r);
                }
                builder.finishRow();
            }
            try (
                EscfBatch source = builder.build();
                EscfBatchScatterer scatterer = new EscfBatchScatterer(BytesRefRecycler.NON_RECYCLING_INSTANCE)
            ) {
                EscfBatch[] parts = scatterer.scatter(source, partitionIds, partitions);
                try {
                    for (int p = 0; p < partitions; p++) {
                        List<String> expected = new ArrayList<>();
                        for (int r = 0; r < rows; r++) {
                            if (partitionIds[r] == p) {
                                expected.add(values.get(r));
                            }
                        }
                        if (expected.isEmpty()) {
                            assertNull(parts[p]);
                            continue;
                        }
                        assertThat(parts[p].column(0), instanceOf(EscfDictStringColumn.class));
                        assertValues(expected, parts[p].column(0));
                    }
                } finally {
                    for (EscfBatch part : parts) {
                        if (part != null) {
                            part.close();
                        }
                    }
                }
            }
        }
    }

    public void testParseRejectsOutOfRangeOrdinal() {
        EscfColumnBuilder builder = dictBuilder();
        addString(builder, "a");
        addString(builder, "b");
        EscfColumnData data = builder.finish(2);
        char[] corrupt = data.ordinals().clone();
        corrupt[1] = 7;
        EscfColumnData bad = EscfColumnData.ofDictString(2, null, data.offsets(), data.data(), corrupt);

        org.elasticsearch.sourcebatch.SourceSchema schema = new org.elasticsearch.sourcebatch.SourceSchema();
        schema.appendLeaf("f", 0);
        BytesReference bytes = EscfBatchCodec.serialize(schema, 2, new EscfColumnData[] { bad });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> EscfBatch.parse(bytes, () -> {}));
        assertThat(e.getMessage(), containsString("out of range"));
    }
}
