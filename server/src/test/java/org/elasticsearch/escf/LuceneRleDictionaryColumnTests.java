/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.sourcebatch.MappedColumns;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests that {@link LuceneRleDictionaryColumn} exposes a run-length encoded column to Lucene as one dictionary entry per run,
 * including the duplicate entries that arise when a long run is split or the same value recurs.
 */
public class LuceneRleDictionaryColumnTests extends ESTestCase {

    private static final String[] VALUES = { "a", "b", "c" };

    /** Untokenized, indexed keyword with sorted-set doc values, the shape of a keyword mapper's field. */
    private static final FieldType KEYWORD_TYPE = new FieldType();
    static {
        KEYWORD_TYPE.setIndexOptions(IndexOptions.DOCS);
        KEYWORD_TYPE.setTokenized(false);
        KEYWORD_TYPE.setDocValuesType(DocValuesType.SORTED_SET);
        KEYWORD_TYPE.freeze();
    }

    /** Builds a column where each stretch is {@code (value index, length)}; a negative index is an absent stretch. */
    private static EscfColumnData build(int[][] stretches, int[] expectedOut) {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, BytesRefRecycler.NON_RECYCLING_INSTANCE, true);
        b.hintScalar(EscfColumnKind.STRING);
        int row = 0;
        for (int[] s : stretches) {
            if (s[0] >= 0) {
                byte[] bytes = VALUES[s[0]].getBytes(StandardCharsets.UTF_8);
                b.setStringRepeated(row, bytes, 0, bytes.length, s[1]);
            }
            for (int i = 0; i < s[1]; i++) {
                expectedOut[row + i] = s[0];
            }
            row += s[1];
        }
        return b.finish(row);
    }

    private static int[][] randomStretches() {
        int n = randomIntBetween(1, 20);
        int[][] stretches = new int[n][];
        for (int i = 0; i < n; i++) {
            int value = randomIntBetween(-1, VALUES.length - 1);
            stretches[i] = new int[] { value, randomBoolean() ? randomIntBetween(2, 5) : randomIntBetween(2, 700) };
        }
        return stretches;
    }

    private static int totalRows(int[][] stretches) {
        int rows = 0;
        for (int[] s : stretches) {
            rows += s[1];
        }
        return rows;
    }

    private static void assertTuples(LuceneRleDictionaryColumn col, int[] expected, int from, int count, FixedBitSet filter) {
        List<BytesRef> dictionary = col.dictionary();
        var tuples = col.tuples();
        int next = from;
        for (int doc = tuples.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = tuples.nextDoc()) {
            while (next < doc + from) {
                assertTrue(
                    "row " + next + " should be absent or filtered",
                    expected[next] < 0 || filter != null && filter.get(next - from) == false
                );
                next++;
            }
            assertEquals(VALUES[expected[doc + from]], dictionary.get(tuples.ordValue()).utf8ToString());
            next++;
        }
        while (next < from + count) {
            assertTrue(
                "row " + next + " should be absent or filtered",
                expected[next] < 0 || filter != null && filter.get(next - from) == false
            );
            next++;
        }
    }

    public void testTuplesMatchSource() {
        for (int iter = 0; iter < 30; iter++) {
            int[][] stretches = randomStretches();
            int[] expected = new int[totalRows(stretches)];
            try (EscfColumnData data = build(stretches, expected)) {
                LuceneRleDictionaryColumn col = LuceneRleDictionaryColumn.of(EscfColumn.from(data), "f", KEYWORD_TYPE);
                if (col == null) {
                    continue; // too few repeats
                }
                assertTuples(col, expected, 0, expected.length, null);

                int from = randomIntBetween(0, expected.length - 1);
                int count = randomIntBetween(1, expected.length - from);
                assertTuples(col.slice(from, count), expected, from, count, null);

                FixedBitSet filter = new FixedBitSet(expected.length);
                for (int i = 0; i < expected.length; i++) {
                    if (randomBoolean()) {
                        filter.set(i);
                    }
                }
                assertTuples(col.withFilter(filter), expected, 0, expected.length, filter);
            }
        }
    }

    public void testSplitRunProducesDuplicateDictionaryEntries() {
        int[] expected = new int[300];
        try (EscfColumnData data = build(new int[][] { { 1, 300 } }, expected)) {
            LuceneRleDictionaryColumn col = LuceneRleDictionaryColumn.of(EscfColumn.from(data), "f", KEYWORD_TYPE);
            assertNotNull(col);
            assertEquals(List.of(new BytesRef("b"), new BytesRef("b")), col.dictionary());
            var ords = col.values();
            for (int i = 0; i < 300; i++) {
                assertEquals(i < 256 ? 0 : 1, ords.nextOrd());
            }
        }
    }

    public void testDeclinesWithoutRepeats() {
        int[] expected = new int[3];
        try (EscfColumnData data = build(new int[][] { { 0, 1 }, { 1, 1 }, { 2, 1 } }, expected)) {
            assertNull(LuceneRleDictionaryColumn.of(EscfColumn.from(data), "f", KEYWORD_TYPE));
        }
    }

    public void testRowFieldCursorEmitsOneFieldPerPresentRow() {
        int[] expected = new int[300 + 4];
        try (EscfColumnData data = build(new int[][] { { 2, 300 }, { -1, 4 } }, expected)) {
            LuceneRleDictionaryColumn col = LuceneRleDictionaryColumn.of(EscfColumn.from(data), "f", StringField.TYPE_NOT_STORED);
            assertNotNull(col);
            LuceneColumn.RowFieldCursor cursor = col.rowFieldCursor();
            int rows = 0;
            for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                assertEquals(rows, doc);
                List<IndexableField> fields = new ArrayList<>();
                cursor.appendCurrentFields(fields);
                assertEquals(1, fields.size());
                assertEquals("c", fields.get(0).binaryValue().utf8ToString());
                rows++;
            }
            assertEquals(300, rows);
        }
    }

    /** Indexes the column through {@code IndexWriter#addBatch} and checks doc count, postings and sorted-set doc values per document. */
    public void testIndexedByLucene() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            int[][] stretches = randomStretches();
            int[] expected = new int[totalRows(stretches)];
            try (EscfColumnData data = build(stretches, expected)) {
                LuceneRleDictionaryColumn col = LuceneRleDictionaryColumn.of(EscfColumn.from(data), "f", KEYWORD_TYPE);
                if (col == null) {
                    continue;
                }
                int from = randomBoolean() ? 0 : randomIntBetween(0, expected.length - 1);
                int count = from == 0 ? expected.length : randomIntBetween(1, expected.length - from);
                FixedBitSet filter = null;
                if (randomBoolean()) {
                    filter = new FixedBitSet(count);
                    for (int i = 0; i < count; i++) {
                        if (randomInt(3) != 0) {
                            filter.set(i);
                        }
                    }
                }
                LuceneRleDictionaryColumn indexed = from == 0 && count == expected.length ? col : col.slice(from, count);
                if (filter != null) {
                    indexed = indexed.withFilter(filter);
                }
                int[] expectedCounts = new int[VALUES.length];
                for (int i = 0; i < count; i++) {
                    boolean kept = filter == null || filter.get(i);
                    if (kept) {
                        if (expected[from + i] >= 0) {
                            expectedCounts[expected[from + i]]++;
                        }
                    }
                }
                assertIndexed(indexed, count, expectedCounts);
            }
        }
    }

    private static void assertIndexed(LuceneRleDictionaryColumn column, int docCount, int[] expectedCounts) throws IOException {
        BytesRef zero = new BytesRef(new byte[8]);
        MappedColumns columns = new MappedColumns(0, docCount, zero, zero, zero, List.of(column));
        IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
        try (Directory dir = newDirectory(); IndexWriter iw = new IndexWriter(dir, iwc)) {
            iw.addBatch(columns.toColumnBatch());
            try (DirectoryReader reader = DirectoryReader.open(iw)) {
                assertEquals(docCount, reader.numDocs());
                for (int v = 0; v < VALUES.length; v++) {
                    assertEquals(VALUES[v], expectedCounts[v], reader.docFreq(new Term("f", VALUES[v])));
                }
            }
        }
    }
}
