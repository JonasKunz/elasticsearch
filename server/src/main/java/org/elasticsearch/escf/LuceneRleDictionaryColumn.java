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

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.escf.EscfColumn.windowValidity;

/**
 * A Lucene {@link DictionaryColumn} over a run-length-encoded ESCF STRING column ({@link EscfRleColumn}). Lucene is handed one
 * dictionary entry per <em>run</em> and, per document, the ordinal of its run, so the column is never expanded to one value per
 * row and a {@code SORTED} or {@code SORTED_SET} doc values writer resolves each run's value once instead of once per row.
 *
 * <p>The dictionary has duplicates whenever a value occurs in several runs, which Lucene permits: it maps equal entries to the
 * same term, at the cost of one hash lookup per used run.
 *
 * <p>Only the doc values pass consumes the ordinals directly. Stored fields and term inversion always go through Lucene's
 * row-oriented pass, which reads the dictionary back one value per row; that still avoids the expansion but not the per-row work.
 *
 * <p>Create via {@link #of}, which declines (returns {@code null}) for anything Lucene's {@link DictionaryColumn} cannot feed or
 * that has too few repeats to be worth it, so callers fall back to {@link LuceneBinaryColumn}.
 */
public final class LuceneRleDictionaryColumn extends DictionaryColumn implements LuceneColumn {

    private final EscfRleColumn data;
    private final FixedBitSet filter;

    private LuceneRleDictionaryColumn(
        EscfRleColumn data,
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
     * Creates a column over {@code source}, or returns {@code null} when {@code source} is not a run-length-encoded STRING column
     * with at least two documents per run on average, or Lucene could not index it as a {@link DictionaryColumn} with
     * {@code fieldType}: the field type is tokenized, has points or vectors, or has doc values other than {@code NONE},
     * {@code SORTED} and {@code SORTED_SET}, or a value is too large for a term.
     */
    @Nullable
    public static LuceneRleDictionaryColumn of(EscfColumn source, String name, IndexableFieldType fieldType) {
        if (source instanceof EscfRleColumn rle && rle.kind() == EscfColumnKind.STRING) {
            return of(rle, name, fieldType);
        }
        return null;
    }

    @Nullable
    private static LuceneRleDictionaryColumn of(EscfRleColumn column, String name, IndexableFieldType fieldType) {
        DocValuesType docValuesType = fieldType.docValuesType();
        boolean supportedDocValues = docValuesType == DocValuesType.NONE
            || docValuesType == DocValuesType.SORTED
            || docValuesType == DocValuesType.SORTED_SET;
        if (supportedDocValues == false
            || fieldType.tokenized()
            || fieldType.pointDimensionCount() != 0
            || fieldType.vectorDimension() != 0
            || column.runCount() == 0
            || column.runCount() * 2L > column.docCount()) {
            return null;
        }
        BytesRef[] entries = new BytesRef[column.runCount()];
        for (int run = 0; run < entries.length; run++) {
            // an absent run has no value; its entry is never referenced by a document
            entries[run] = column.runs().isAbsent(run) ? new BytesRef() : column.runs().getBinaryValue(run);
            if (entries[run].length > ByteBlockPool.BYTE_BLOCK_SIZE - 2) {
                return null;
            }
        }
        return new LuceneRleDictionaryColumn(column, name, fieldType, densityOf(column), null, Arrays.asList(entries));
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
                return data.runOf(doc);
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
                return data.runOf(row++);
            }
        };
    }

    @Override
    public LuceneRleDictionaryColumn withFilter(FixedBitSet filter) {
        assert filter == null || filter.length() == data.docCount;
        return new LuceneRleDictionaryColumn(
            data,
            name(),
            fieldType(),
            densityOf(data),
            LuceneColumn.singleFilter(this.filter, filter),
            dictionary()
        );
    }

    @Override
    public LuceneRleDictionaryColumn slice(int from, int count) {
        EscfRleColumn sliced = (EscfRleColumn) data.sliceInternal(from, count);
        BytesRef[] entries = new BytesRef[Math.max(1, sliced.runCount())];
        for (int run = 0; run < sliced.runCount(); run++) {
            entries[run] = sliced.runs().isAbsent(run) ? new BytesRef() : sliced.runs().getBinaryValue(run);
        }
        if (sliced.runCount() == 0) {
            entries[0] = new BytesRef(); // Lucene requires a non-empty dictionary; an empty window never references it
        }
        return new LuceneRleDictionaryColumn(
            sliced,
            name(),
            fieldType(),
            densityOf(sliced),
            windowValidity(filter, from, count),
            Arrays.asList(entries)
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
