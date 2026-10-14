/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.List;

/**
 * Tests for run-length encoded columns: the {@link EscfColumnBuilder} run splitting, the reader's equivalence to a plain column,
 * slicing, and the wire round trip.
 */
public class EscfRleColumnTests extends ESTestCase {

    private static EscfColumnBuilder rleBuilder() {
        return new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, BytesRefRecycler.NON_RECYCLING_INSTANCE, true);
    }

    private static EscfColumnBuilder plainBuilder() {
        return new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, BytesRefRecycler.NON_RECYCLING_INSTANCE, false);
    }

    public void testLongRunsAreCollapsed() {
        EscfColumnBuilder b = rleBuilder();
        b.setLongRepeated(0, 7L, 5);
        b.setLongRepeated(5, 9L, 3);
        try (EscfColumnData data = b.finish(8)) {
            assertTrue(data.isRunLengthEncoded());
            assertEquals(2, data.runCount());
            assertEquals(8, data.docCount());
            EscfColumn col = EscfColumn.from(data);
            for (int i = 0; i < 5; i++) {
                assertEquals(7L, col.getLongValue(i));
            }
            for (int i = 5; i < 8; i++) {
                assertEquals(9L, col.getLongValue(i));
            }
        }
    }

    public void testLongRunsLongerThanOneByteAreSplit() {
        for (int[] c : new int[][] { { 256, 1 }, { 257, 2 }, { 300, 2 }, { 512, 2 }, { 513, 3 }, { 1000, 4 } }) {
            EscfColumnBuilder b = rleBuilder();
            b.setLongRepeated(0, 42L, c[0]);
            try (EscfColumnData data = b.finish(c[0])) {
                assertEquals("rows=" + c[0], c[1], data.runCount());
                EscfColumn col = EscfColumn.from(data);
                assertEquals(c[0], col.docCount);
                for (int i = 0; i < c[0]; i++) {
                    assertEquals(42L, col.getLongValue(i));
                }
                EscfRleColumn rle = (EscfRleColumn) col;
                assertEquals(Math.min(256, c[0]), rle.runLength(0));
                if (c[0] == 300) {
                    assertEquals(44, rle.runLength(1));
                }
            }
        }
    }

    public void testAbsentRows() {
        EscfColumnBuilder b = rleBuilder();
        b.setLongRepeated(0, 1L, 3);
        b.setLongRepeated(10, 1L, 4);
        try (EscfColumnData data = b.finish(20)) {
            EscfColumn col = EscfColumn.from(data);
            assertEquals(20, col.docCount);
            for (int i = 0; i < 20; i++) {
                boolean present = i < 3 || (i >= 10 && i < 14);
                assertEquals("row " + i, present == false, col.isAbsent(i));
                if (present) {
                    assertEquals(1L, col.getLongValue(i));
                }
            }
        }
    }

    public void testRandomizedEquivalenceWithPlainColumn() {
        for (int iter = 0; iter < 50; iter++) {
            int rows = randomIntBetween(1, 1500);
            boolean strings = randomBoolean();
            EscfColumnBuilder rle = rleBuilder();
            EscfColumnBuilder plain = plainBuilder();
            if (strings) {
                rle.hintScalar(EscfColumnKind.STRING);
                plain.hintScalar(EscfColumnKind.STRING);
            }
            int row = 0;
            while (row < rows) {
                int len = Math.min(rows - row, randomBoolean() ? randomIntBetween(1, 5) : randomIntBetween(1, 700));
                if (randomInt(4) == 0) {
                    row += len; // absent stretch
                    continue;
                }
                int distinct = randomIntBetween(0, 3);
                if (strings) {
                    byte[] bytes = ("v" + distinct).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    rle.setStringRepeated(row, bytes, 0, bytes.length, len);
                    plain.setStringRepeated(row, bytes, 0, bytes.length, len);
                } else {
                    rle.setLongRepeated(row, distinct, len);
                    plain.setLongRepeated(row, distinct, len);
                }
                row += len;
            }
            try (EscfColumnData rleData = rle.finish(rows); EscfColumnData plainData = plain.finish(rows)) {
                EscfColumn r = EscfColumn.from(rleData);
                EscfColumn p = EscfColumn.from(plainData);
                assertEqualColumns(p, r, rows, strings);

                int from = randomIntBetween(0, rows - 1);
                int count = randomIntBetween(0, rows - from);
                assertEqualColumns(p.sliceInternal(from, count), r.sliceInternal(from, count), count, strings);

                // the plain layout handed to consumers is identical to what a plain builder produces
                EscfColumn expanded = EscfColumn.from(r.columnData());
                assertFalse(expanded instanceof EscfRleColumn);
                assertEqualColumns(p, expanded, rows, strings);

                // native layout round trips
                assertEqualColumns(p, EscfColumn.from(r.toColumnData()), rows, strings);
            }
        }
    }

    private static void assertEqualColumns(EscfColumn expected, EscfColumn actual, int rows, boolean strings) {
        assertEquals(expected.docCount, actual.docCount);
        assertEquals(expected.kind(), actual.kind());
        for (int i = 0; i < rows; i++) {
            assertEquals("absent " + i, expected.isAbsent(i), actual.isAbsent(i));
            if (expected.isPresent(i)) {
                if (strings) {
                    assertEquals("row " + i, expected.getBinaryValue(i), actual.getBinaryValue(i));
                    assertEquals("row " + i, expected.getStringValue(i), actual.getStringValue(i));
                } else {
                    assertEquals("row " + i, expected.getLongValue(i), actual.getLongValue(i));
                }
            }
        }
        if (strings) {
            var e = expected.bytesRefCursor(true);
            var a = actual.bytesRefCursor(true);
            for (int doc = e.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = e.nextDoc()) {
                assertEquals(doc, a.nextDoc());
                assertEquals(BytesRef.deepCopyOf(e.value()), BytesRef.deepCopyOf(a.value()));
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, a.nextDoc());
        } else {
            var e = expected.longCursor();
            var a = actual.longCursor();
            for (int doc = e.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = e.nextDoc()) {
                assertEquals(doc, a.nextDoc());
                assertEquals(e.longValue(), a.longValue());
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, a.nextDoc());
        }
    }

    public void testWireRoundTrip() {
        SourceSchema schema = new SourceSchema(List.of(""), new int[] { 0 }, List.of("x"), new int[] { 0 });
        EscfColumnBuilder b = rleBuilder();
        b.setLongRepeated(0, 5L, 300);
        b.setLongRepeated(310, 6L, 10);
        EscfColumnData data = b.finish(320);
        int runs = data.runCount();
        assertEquals("256 + 44 + absent run + 10", 4, runs);
        try (EscfBatch batch = EscfBatch.of(schema, 320, new EscfColumnData[] { data })) {
            try (EscfBatch parsed = EscfBatchCodec.parse(batch.data(), () -> {})) {
                assertEquals(320, parsed.docCount());
                EscfColumn col = parsed.column(0);
                assertTrue(col instanceof EscfRleColumn);
                assertEquals(runs, ((EscfRleColumn) col).runCount());
                for (int i = 0; i < 320; i++) {
                    if (i < 300) {
                        assertEquals(5L, col.getLongValue(i));
                    } else if (i < 310) {
                        assertTrue(col.isAbsent(i));
                    } else {
                        assertEquals(6L, col.getLongValue(i));
                    }
                }
            }
        }
    }

    public void testPromotionToUnionExpandsRuns() {
        EscfColumnBuilder b = rleBuilder();
        b.setLongRepeated(0, 1L, 300);
        b.setString(300, new BytesRef("a"));
        try (EscfColumnData data = b.finish(301)) {
            EscfColumn col = EscfColumn.from(data);
            assertEquals(301, col.docCount);
            for (int i = 0; i < 300; i++) {
                assertEquals(1L, col.getLongValue(i));
            }
            assertEquals(new BytesRef("a"), col.getBinaryValue(300));
        }
    }

    public void testRepeatedRunsAreMergedAcrossCalls() {
        EscfColumnBuilder b = rleBuilder();
        b.setLongRepeated(0, 3L, 100);
        b.setLongRepeated(100, 3L, 100);
        try (EscfColumnData data = b.finish(200)) {
            assertEquals(1, data.runCount());
        }
    }

    /** Lucene reads fixed-width columns through their concrete class, so native run-length-encoded data must be handed over expanded. */
    public void testLuceneColumnsAcceptNativeRleData() {
        EscfColumnBuilder longs = rleBuilder();
        longs.setLongRepeated(0, 5L, 300);
        EscfColumnBuilder strings = rleBuilder();
        strings.hintScalar(EscfColumnKind.STRING);
        strings.setStringRepeated(0, new byte[] { 'a' }, 0, 1, 300);
        try (EscfColumnData longData = longs.finish(300); EscfColumnData stringData = strings.finish(300)) {
            LuceneLongColumn longColumn = LuceneLongColumn.of(
                EscfColumn.from(longData).toColumnData(),
                "l",
                org.apache.lucene.document.NumericDocValuesField.TYPE,
                org.apache.lucene.document.column.LongColumn.NumericKind.LONG
            );
            var cursor = longColumn.values();
            for (int i = 0; i < 300; i++) {
                assertEquals(5L, cursor.nextLong());
            }
            LuceneBinaryColumn binaryColumn = LuceneBinaryColumn.of(
                EscfColumn.from(stringData).toColumnData(),
                "s",
                org.apache.lucene.document.StringField.TYPE_NOT_STORED
            );
            assertNotNull(binaryColumn.values());
        }
    }
}
