/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.DictionaryColumn;
import org.apache.lucene.document.column.OrdinalsCursor;
import org.apache.lucene.document.column.OrdinalsTupleCursor;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.sourcebatch.LuceneColumn;

import java.util.List;

import static org.elasticsearch.escf.EscfColumn.windowValidity;

/**
 * A Lucene {@link DictionaryColumn} over a dictionary-encoded ESCF STRING column. Lucene is handed the dictionary and the ordinals
 * as they are stored, so the column is never expanded to one value per row, and a {@code SORTED_SET} or {@code SORTED} doc values
 * writer resolves each distinct value once per batch instead of once per row.
 *
 * <p>Only the doc values pass consumes the ordinals directly. Stored fields and term inversion always go through Lucene's
 * row-oriented pass, which reads the dictionary back one value per row; that still avoids the expansion but not the per-row work.
 *
 * <p>Create via {@link #of}, which declines (returns {@code null}) for anything Lucene's {@link DictionaryColumn} cannot feed, so
 * callers fall back to {@link LuceneBinaryColumn}.
 */
public final class LuceneDictionaryColumn extends DictionaryColumn implements LuceneColumn {

    private final EscfDictStringColumn data;
    private final FixedBitSet filter;

    private LuceneDictionaryColumn(
        EscfDictStringColumn data,
        String name,
        IndexableFieldType fieldType,
        Density density,
        FixedBitSet filter,
        List<BytesRef> dictionary
    ) {
        super(name, fieldType, filter != null ? Density.SPARSE : density, dictionary);
        this.data = data;
        this.filter = filter;
    }

    /**
     * Creates a column over {@code source}, or returns {@code null} when {@code source} is not dictionary-encoded or Lucene could not
     * index it as a {@link DictionaryColumn} with {@code fieldType}: the dictionary is empty or has an entry too large for a term,
     * the field type is tokenized, has points or vectors, or has doc values other than {@code NONE}, {@code SORTED} and
     * {@code SORTED_SET}.
     */
    @Nullable
    public static LuceneDictionaryColumn of(EscfColumn source, String name, IndexableFieldType fieldType) {
        if (source instanceof EscfDictStringColumn dictionaryColumn) {
            return of(dictionaryColumn, name, fieldType);
        }
        return null;
    }

    @Nullable
    private static LuceneDictionaryColumn of(EscfDictStringColumn dictionaryColumn, String name, IndexableFieldType fieldType) {
        DocValuesType docValuesType = fieldType.docValuesType();
        boolean supportedDocValues = docValuesType == DocValuesType.NONE
            || docValuesType == DocValuesType.SORTED
            || docValuesType == DocValuesType.SORTED_SET;
        if (supportedDocValues == false
            || fieldType.tokenized()
            || fieldType.pointDimensionCount() != 0
            || fieldType.vectorDimension() != 0
            || dictionaryColumn.dictionarySize() == 0
            || dictionaryColumn.maxEntryLength() > ByteBlockPool.BYTE_BLOCK_SIZE - 2) {
            return null;
        }
        return new LuceneDictionaryColumn(
            dictionaryColumn,
            name,
            fieldType,
            densityOf(dictionaryColumn),
            null,
            dictionaryColumn.dictionaryEntries()
        );
    }

    private static Density densityOf(EscfColumn data) {
        return data.validity != null ? Density.SPARSE : Density.DENSE;
    }

    @Override
    public OrdinalsTupleCursor tuples() {
        final PresentDocIterator present = data.presentDocs();
        return new OrdinalsTupleCursor() {
            private int doc;

            @Override
            public int nextDoc() {
                while ((doc = present.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (filter == null || filter.get(doc)) {
                        return doc;
                    }
                }
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int ordValue() {
                return data.ordinal(doc);
            }
        };
    }

    @Override
    public OrdinalsCursor values() {
        if (density() == Density.SPARSE) {
            return super.values();
        }
        return new OrdinalsCursor(data.docCount()) {
            private int row;

            @Override
            public int nextOrd() {
                return data.ordinal(row++);
            }
        };
    }

    @Override
    public LuceneDictionaryColumn withFilter(FixedBitSet filter) {
        assert filter == null || filter.length() == data.docCount;
        return new LuceneDictionaryColumn(
            data,
            name(),
            fieldType(),
            densityOf(data),
            LuceneColumn.singleFilter(this.filter, filter),
            dictionary()
        );
    }

    @Override
    public LuceneDictionaryColumn slice(int from, int count) {
        EscfDictStringColumn sliced = (EscfDictStringColumn) data.sliceInternal(from, count);
        return new LuceneDictionaryColumn(
            sliced,
            name(),
            fieldType(),
            densityOf(sliced),
            windowValidity(filter, from, count),
            dictionary()
        );
    }

    @Override
    public Column toLuceneColumn() {
        return this;
    }

    @Override
    public LuceneColumn.RowFieldCursor rowFieldCursor() {
        // retainValues=true: the emitted Fields outlive the cursor position.
        final var cursor = data.bytesRefCursor(true);
        return new LuceneColumn.RowFieldCursor() {
            private int doc = DocIdSetIterator.NO_MORE_DOCS;

            @Override
            public int nextDoc() {
                int next;
                while ((next = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (filter == null || filter.get(next)) {
                        return doc = next;
                    }
                }
                return doc = DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public void appendCurrentFields(List<? super IndexableField> out) {
                out.add(new Field(name(), cursor.value(), fieldType()));
            }
        };
    }
}
