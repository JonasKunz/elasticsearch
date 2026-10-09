/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.sourcebatch.LuceneColumn;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Indexes a {@link LuceneDictionaryColumn} through Lucene's {@code IndexWriter#addBatch} and checks that what lands in the index is
 * what the same values produce through a {@link LuceneBinaryColumn}.
 */
public class LuceneDictionaryColumnTests extends ESTestCase {

    private static final String FIELD = "f";

    private static FieldType fieldType(DocValuesType docValuesType, boolean indexed, boolean stored) {
        FieldType type = new FieldType();
        type.setTokenized(false);
        type.setIndexOptions(indexed ? IndexOptions.DOCS : IndexOptions.NONE);
        type.setDocValuesType(docValuesType);
        type.setStored(stored);
        type.freeze();
        return type;
    }

    private static EscfColumn dictionaryColumn(List<String> values) {
        EscfColumnBuilder builder = new EscfColumnBuilder(
            EscfColumnBuilder.CollisionPolicy.SPLIT,
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            true
        );
        for (String value : values) {
            if (value == null) {
                builder.addAbsent();
            } else {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                builder.addString(new XContentString.UTF8Bytes(bytes, 0, bytes.length));
            }
        }
        EscfColumn column = EscfColumn.from(builder.finish(values.size()));
        assertThat(column, instanceOf(EscfDictStringColumn.class));
        return column;
    }

    private static List<String> randomValues(int rows, boolean withAbsents) {
        List<String> values = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            values.add(withAbsents && randomInt(3) == 0 ? null : "value-" + randomInt(6));
        }
        if (values.stream().allMatch(v -> v == null)) {
            values.set(0, "value-0"); // an all-absent column has no kind, so it is not a dictionary column
        }
        return values;
    }

    /** What an index holds for the field, per document: stored value, sorted(-set) doc values, and the terms with their doc ids. */
    private record Indexed(List<String> stored, List<List<String>> docValues, Map<String, List<Integer>> postings) {}

    private static Indexed index(Column column, int numDocs) throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig())) {
            writer.addBatch(new ColumnBatch() {
                @Override
                public int numDocs() {
                    return numDocs;
                }

                @Override
                public Iterable<Column> columns() {
                    return List.of(column);
                }
            });
            writer.commit();
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(1, reader.leaves().size());
                LeafReader leaf = reader.leaves().get(0).reader();
                int maxDoc = leaf.maxDoc();

                List<String> stored = new ArrayList<>();
                for (int doc = 0; doc < maxDoc; doc++) {
                    Document document = leaf.storedFields().document(doc);
                    BytesRef value = document.getBinaryValue(FIELD);
                    stored.add(value == null ? null : value.utf8ToString());
                }

                List<List<String>> docValues = new ArrayList<>();
                for (int doc = 0; doc < maxDoc; doc++) {
                    docValues.add(new ArrayList<>());
                }
                DocValuesType docValuesType = column.fieldType().docValuesType();
                if (docValuesType == DocValuesType.SORTED_SET) {
                    SortedSetDocValues dv = leaf.getSortedSetDocValues(FIELD);
                    for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
                        for (int i = 0; i < dv.docValueCount(); i++) {
                            docValues.get(doc).add(dv.lookupOrd(dv.nextOrd()).utf8ToString());
                        }
                    }
                } else if (docValuesType == DocValuesType.SORTED) {
                    SortedDocValues dv = leaf.getSortedDocValues(FIELD);
                    for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
                        docValues.get(doc).add(dv.lookupOrd(dv.ordValue()).utf8ToString());
                    }
                }

                Map<String, List<Integer>> postings = new TreeMap<>();
                Terms terms = leaf.terms(FIELD);
                if (terms != null) {
                    TermsEnum termsEnum = terms.iterator();
                    for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                        var docs = termsEnum.postings(null);
                        List<Integer> ids = new ArrayList<>();
                        for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
                            ids.add(doc);
                        }
                        postings.put(term.utf8ToString(), ids);
                    }
                }
                return new Indexed(stored, docValues, postings);
            }
        }
    }

    private static LuceneBinaryColumn plainColumn(EscfColumn dictionary, FieldType type) {
        return LuceneBinaryColumn.of(dictionary.columnData(), FIELD, type);
    }

    private void assertSameAsPlain(
        EscfColumn dictionary,
        FieldType type,
        int numDocs,
        java.util.function.UnaryOperator<LuceneColumn> adjust
    ) throws IOException {
        LuceneDictionaryColumn dictionaryColumn = LuceneDictionaryColumn.of(dictionary, FIELD, type);
        assertNotNull(dictionaryColumn);
        LuceneColumn plain = plainColumn(dictionary, type);
        Indexed expected = index(adjust.apply(plain).toLuceneColumn(), numDocs);
        Indexed actual = index(adjust.apply(dictionaryColumn).toLuceneColumn(), numDocs);
        assertEquals(expected, actual);
    }

    public void testSortedSetDocValuesTermsAndStoredMatchPlainColumn() throws IOException {
        FieldType type = fieldType(DocValuesType.SORTED_SET, true, true);
        for (boolean withAbsents : new boolean[] { false, true }) {
            int rows = randomIntBetween(1, 100);
            EscfColumn dictionary = dictionaryColumn(randomValues(rows, withAbsents));
            assertSameAsPlain(dictionary, type, rows, c -> c);
        }
    }

    public void testSortedDocValuesMatchPlainColumn() throws IOException {
        FieldType type = fieldType(DocValuesType.SORTED, false, false);
        for (boolean withAbsents : new boolean[] { false, true }) {
            int rows = randomIntBetween(1, 100);
            EscfColumn dictionary = dictionaryColumn(randomValues(rows, withAbsents));
            assertSameAsPlain(dictionary, type, rows, c -> c);
        }
    }

    public void testTermsOnlyMatchPlainColumn() throws IOException {
        FieldType type = fieldType(DocValuesType.NONE, true, false);
        int rows = randomIntBetween(1, 100);
        EscfColumn dictionary = dictionaryColumn(randomValues(rows, true));
        assertSameAsPlain(dictionary, type, rows, c -> c);
    }

    public void testSliceMatchesPlainColumn() throws IOException {
        FieldType type = fieldType(DocValuesType.SORTED_SET, true, true);
        List<String> values = randomValues(80, true);
        EscfColumn dictionary = dictionaryColumn(values);
        int from = randomIntBetween(0, 40);
        int count = randomIntBetween(1, 40);
        assertSameAsPlain(dictionary, type, count, c -> c.slice(from, count));
    }

    public void testFilterMatchesPlainColumn() throws IOException {
        for (DocValuesType docValuesType : new DocValuesType[] { DocValuesType.SORTED_SET, DocValuesType.SORTED }) {
            FieldType type = fieldType(docValuesType, true, true);
            List<String> values = randomValues(60, randomBoolean());
            EscfColumn dictionary = dictionaryColumn(values);
            FixedBitSet filter = new FixedBitSet(values.size());
            for (int i = 0; i < values.size(); i++) {
                if (randomBoolean()) {
                    filter.set(i);
                }
            }
            assertSameAsPlain(dictionary, type, values.size(), c -> c.withFilter(filter));
        }
    }

    public void testRowFieldCursorMatchesPlainColumn() {
        FieldType type = fieldType(DocValuesType.SORTED_SET, true, true);
        EscfColumn dictionary = dictionaryColumn(randomValues(50, true));
        LuceneDictionaryColumn dictionaryColumn = LuceneDictionaryColumn.of(dictionary, FIELD, type);
        LuceneBinaryColumn plain = plainColumn(dictionary, type);
        assertEquals(rows(plain), rows(dictionaryColumn));
    }

    private static Map<Integer, String> rows(LuceneColumn column) {
        Map<Integer, String> rows = new TreeMap<>();
        LuceneColumn.RowFieldCursor cursor = column.rowFieldCursor();
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
            List<org.apache.lucene.index.IndexableField> fields = new ArrayList<>();
            cursor.appendCurrentFields(fields);
            assertEquals(1, fields.size());
            rows.put(doc, fields.get(0).binaryValue().utf8ToString());
        }
        return rows;
    }

    public void testDeclinesWhatLuceneCannotIndexFromADictionary() {
        EscfColumn dictionary = dictionaryColumn(List.of("a", "b"));
        assertThat(LuceneDictionaryColumn.of(dictionary, FIELD, fieldType(DocValuesType.BINARY, true, false)), nullValue());
        assertThat(LuceneDictionaryColumn.of(dictionary, FIELD, fieldType(DocValuesType.SORTED_NUMERIC, false, false)), nullValue());
        FieldType tokenized = new FieldType();
        tokenized.setTokenized(true);
        tokenized.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        tokenized.freeze();
        assertThat(LuceneDictionaryColumn.of(dictionary, FIELD, tokenized), nullValue());

        // a plain column and an empty dictionary are not candidates either
        EscfColumnBuilder plainBuilder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        plainBuilder.addString(new XContentString.UTF8Bytes(new byte[] { 'a' }, 0, 1));
        assertThat(
            LuceneDictionaryColumn.of(EscfColumn.from(plainBuilder.finish(1)), FIELD, fieldType(DocValuesType.SORTED_SET, true, false)),
            nullValue()
        );
        EscfColumnBuilder allAbsent = new EscfColumnBuilder(
            EscfColumnBuilder.CollisionPolicy.SPLIT,
            BytesRefRecycler.NON_RECYCLING_INSTANCE,
            true
        );
        allAbsent.hintScalar(EscfColumnKind.STRING);
        allAbsent.addAbsents(3);
        assertThat(
            LuceneDictionaryColumn.of(EscfColumn.from(allAbsent.finish(3)), FIELD, fieldType(DocValuesType.SORTED_SET, true, false)),
            nullValue()
        );
    }

    public void testDeclinesEntryTooLargeForATerm() {
        String huge = "x".repeat(40_000);
        EscfColumn dictionary = dictionaryColumn(List.of("a", huge));
        assertThat(LuceneDictionaryColumn.of(dictionary, FIELD, fieldType(DocValuesType.SORTED_SET, true, false)), nullValue());
    }
}
