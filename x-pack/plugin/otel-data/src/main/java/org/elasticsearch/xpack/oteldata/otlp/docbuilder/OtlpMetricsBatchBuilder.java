/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.transport.BytesRefRecycler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds an {@link EscfBatch} from OTLP metric data points, one row per data point. It is a specialization of the generic
 * {@link org.elasticsearch.escf.EscfBatchBuilder} that exploits the shape of OTLP metrics data:
 *
 * <ul>
 *   <li><b>Blocks.</b> All data points of one scope of one resource share the resource and scope fields. {@link #beginBlock} declares
 *   how many rows they span and the {@code block*Field} methods then write such a field once for all of them, as a bulk fill
 *   rather than a write per row.</li>
 *   <li><b>No per-row absent filling.</b> A data point holds one metric value out of potentially hundreds of metric columns, so
 *   writing an absent for every other column in every row would dominate. Columns are written positionally instead and the gaps
 *   between written rows are filled in bulk, lazily, by the column builders.</li>
 * </ul>
 *
 * <p>Usage per row: {@link #beginRow()}, field writes, {@link #finishRow()}. Block fields are only written in the first row of a
 * block, and must not be written again in the remaining rows of it. Leaf order in the schema is first-appearance order, as in the
 * generic builder, which determines the field order of the reconstructed {@code _source}.
 */
public final class OtlpMetricsBatchBuilder implements Releasable {

    private static final int INITIAL_CAPACITY = 64;

    private final SourceSchema schema = new SourceSchema();
    private final Recycler<BytesRef> recycler;
    private final List<EscfColumnBuilder> builders = new ArrayList<>(INITIAL_CAPACITY);
    /** Per leaf: {@code 1 + the row it was last written in}, to reject duplicate fields within a row. */
    private int[] writtenInRow = new int[INITIAL_CAPACITY];

    private int[] parentStack = new int[8];
    private int parentDepth;

    private int docCount;
    /** The number of rows in the open block, or {@code 0}. */
    private int blockRows;
    /** The first row of the open block. */
    private int blockStart;

    /** Creates a builder without buffer recycling, for tests. */
    public OtlpMetricsBatchBuilder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE);
    }

    public OtlpMetricsBatchBuilder(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    /** Starts the next row. */
    public void beginRow() {
        parentDepth = 0;
    }

    /** Completes the current row and returns its zero-based index. */
    public int finishRow() {
        int row = docCount++;
        if (blockRows > 0 && docCount == blockStart + blockRows) {
            blockRows = 0;
        }
        return row;
    }

    /**
     * Completes a row that failed partway through. It keeps whatever was written to it; callers discard the whole builder after
     * a failure, so this only has to leave the builder in a consistent state.
     */
    public void abortRow() {
        finishRow();
    }

    /**
     * Declares that the next {@code rows} rows share the values of the block fields that the first of them writes.
     * A block must be complete before the next one starts.
     */
    public void beginBlock(int rows) {
        if (blockRows > 0) {
            throw new IllegalStateException("a block of [" + blockRows + "] rows is still open");
        }
        if (rows <= 0) {
            throw new IllegalArgumentException("a block needs at least one row, got [" + rows + "]");
        }
        blockRows = rows;
        blockStart = docCount;
    }

    public void startObject(String name) {
        int nonLeafIdx = schema.appendNonLeaf(name, parentStack[parentDepth]);
        parentDepth++;
        if (parentDepth == parentStack.length) {
            parentStack = Arrays.copyOf(parentStack, parentStack.length * 2);
        }
        parentStack[parentDepth] = nonLeafIdx;
    }

    public void endObject() {
        assert parentDepth > 0 : "endObject called without matching startObject";
        parentDepth--;
    }

    public void longField(String name, long value) {
        column(name).setLong(docCount, value);
    }

    public void doubleField(String name, double value) {
        column(name).setDouble(docCount, value);
    }

    public void booleanField(String name, boolean value) {
        column(name).setBoolean(docCount, value);
    }

    public void stringField(String name, byte[] bytes, int offset, int length) {
        column(name).setString(docCount, bytes, offset, length);
    }

    /** Writes a long that is the same in every row of the open block; only valid in the first row of the block. */
    public void blockLongField(String name, long value) {
        blockColumn(name).setLongRepeated(docCount, value, blockRows);
    }

    public void blockDoubleField(String name, double value) {
        blockColumn(name).setDoubleRepeated(docCount, value, blockRows);
    }

    public void blockBooleanField(String name, boolean value) {
        blockColumn(name).setBooleanRepeated(docCount, value, blockRows);
    }

    public void blockStringField(String name, byte[] bytes, int offset, int length) {
        blockColumn(name).setStringRepeated(docCount, bytes, offset, length, blockRows);
    }

    private EscfColumnBuilder blockColumn(String name) {
        if (blockRows == 0 || docCount != blockStart) {
            throw new IllegalStateException("block fields can only be written in the first row of a block");
        }
        return column(name);
    }

    /** The number of rows written, including the one in progress if it has been finished. */
    public int docCount() {
        return docCount;
    }

    /**
     * Finalizes the columns, filling the trailing rows in which they were not written with absents, and returns the batch, which
     * owns the column buffers.
     */
    public EscfBatch build() {
        assert blockRows == 0 : "a block of [" + blockRows + "] rows was not completed";
        int leafCount = schema.leafCount();
        EscfColumnData[] columns = new EscfColumnData[leafCount];
        for (int c = 0; c < leafCount; c++) {
            columns[c] = builders.get(c).finish(docCount);
        }
        builders.clear();
        return EscfBatch.of(schema, docCount, columns);
    }

    @Override
    public void close() {
        Releasables.close(builders);
        builders.clear();
    }

    /** The column builder of the leaf {@code name} in the current object, for a write starting at row {@code docCount}. */
    private EscfColumnBuilder column(String name) {
        int colIdx = schema.appendLeaf(name, parentStack[parentDepth]);
        if (colIdx >= writtenInRow.length) {
            writtenInRow = Arrays.copyOf(writtenInRow, Math.max(colIdx + 1, writtenInRow.length * 2));
        }
        if (writtenInRow[colIdx] == docCount + 1) {
            throw new IllegalArgumentException("Duplicate field [" + name + "]");
        }
        writtenInRow[colIdx] = docCount + 1;
        // Rows before the first one a column is written in are absent: the builder fills them in lazily.
        while (builders.size() <= colIdx) {
            builders.add(new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler));
        }
        return builders.get(colIdx);
    }
}
