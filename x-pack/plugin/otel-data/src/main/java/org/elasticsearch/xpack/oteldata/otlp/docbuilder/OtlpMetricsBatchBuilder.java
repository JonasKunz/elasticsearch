/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.transport.BytesRefRecycler;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds an {@link EscfBatch} from OTLP metric data points, one row per data point. It is a specialization of the generic
 * {@link org.elasticsearch.escf.EscfBatchBuilder} that exploits the shape of OTLP metrics data:
 *
 * <ul>
 *   <li><b>Fields written for a range of rows at once.</b> All data points of one scope of one resource share the resource and scope
 *   fields. Such a field is {@link #declareField declared} when its value is first seen, so that it takes its place in the schema,
 *   and written once for all of its rows by a {@code repeated*Field} call once the rows are known. When the data points are
 *   streamed from the wire their number is not known in advance, so the write happens after the last of them; on a
 *   dictionary-encoded string column it costs a fill of the ordinal array rather than a dictionary lookup per row.</li>
 *   <li><b>Fields addressed by handle.</b> {@link #declareField} returns a handle for a leaf of the current object, and the
 *   handle-based setters write to it without looking the name up again. Names can be given as UTF-8 bytes pointing into the
 *   request payload: a handle lookup by bytes does not allocate once the leaf exists, which keeps attribute keys from being
 *   materialized as strings for every data point.</li>
 *   <li><b>No per-row absent filling.</b> A data point holds one metric value out of potentially hundreds of metric columns, so
 *   writing an absent for every other column in every row would dominate. Columns are written positionally instead and the gaps
 *   between written rows are filled in bulk, lazily, by the column builders.</li>
 * </ul>
 *
 * <p>Usage per row: {@link #beginRow()}, field writes, {@link #finishRow()}. Leaf order in the schema is first-appearance order (a
 * declaration counts as an appearance), as in the generic builder, which determines the field order of the reconstructed
 * {@code _source}.
 */
public final class OtlpMetricsBatchBuilder implements Releasable {

    private static final int INITIAL_CAPACITY = 64;

    private final SourceSchema schema = new SourceSchema();
    private final Recycler<BytesRef> recycler;
    private final boolean dictionaryStrings;
    private final List<EscfColumnBuilder> builders = new ArrayList<>(INITIAL_CAPACITY);
    /** Per leaf: {@code 1 + the row it was last written in}, to reject duplicate fields within a row. */
    private int[] writtenInRow = new int[INITIAL_CAPACITY];
    /** Per leaf: its name as UTF-8, for the lookup by bytes. */
    private byte[][] leafNames = new byte[INITIAL_CAPACITY][];
    /** Open-addressing table from (parent, UTF-8 name) to {@code 1 + leaf index}; {@code 0} marks an empty slot. */
    private int[] leafLookup = new int[INITIAL_CAPACITY * 2];
    private int leafLookupSize;

    private int[] parentStack = new int[8];
    private int parentDepth;

    private int docCount;

    /** Creates a builder with dictionary-encoded string columns and no buffer recycling, for tests. */
    public OtlpMetricsBatchBuilder() {
        this(BytesRefRecycler.NON_RECYCLING_INSTANCE, true);
    }

    public OtlpMetricsBatchBuilder(Recycler<BytesRef> recycler, boolean dictionaryStrings) {
        this.recycler = recycler;
        this.dictionaryStrings = dictionaryStrings;
    }

    /** Starts the next row. */
    public void beginRow() {
        parentDepth = 0;
    }

    /** Completes the current row and returns its zero-based index. */
    public int finishRow() {
        return docCount++;
    }

    /**
     * Completes a row that failed partway through. It keeps whatever was written to it; callers discard the whole builder after
     * a failure, so this only has to leave the builder in a consistent state.
     */
    public void abortRow() {
        finishRow();
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

    // -------------------------------------------------------------------------
    // Fields by name
    // -------------------------------------------------------------------------

    public void longField(String name, long value) {
        longField(declareField(name), value);
    }

    public void doubleField(String name, double value) {
        doubleField(declareField(name), value);
    }

    public void booleanField(String name, boolean value) {
        booleanField(declareField(name), value);
    }

    public void stringField(String name, byte[] bytes, int offset, int length) {
        stringField(declareField(name), bytes, offset, length);
    }

    // -------------------------------------------------------------------------
    // Fields by handle
    // -------------------------------------------------------------------------

    /**
     * Declares the leaf {@code name} of the current object, if it is not part of the schema yet, and returns its handle. Nothing
     * is written; the handle is valid for the lifetime of this builder, independent of the object the cursor is in later.
     */
    public int declareField(String name) {
        int leaf = schema.appendLeaf(name, parentStack[parentDepth]);
        ensureLeaf(leaf);
        if (leafNames[leaf] == null) {
            leafNames[leaf] = name.getBytes(StandardCharsets.UTF_8);
            insertLeafLookup(leaf);
        }
        return leaf;
    }

    /**
     * Same as {@link #declareField(String)} for a name given as UTF-8 bytes. The name is only materialized as a string the first
     * time it is seen under the current object.
     */
    public int declareField(byte[] name, int offset, int length) {
        final int parent = parentStack[parentDepth];
        final int mask = leafLookup.length - 1;
        int slot = StringHelper.murmurhash3_x86_32(name, offset, length, parent) & mask;
        int entry;
        while ((entry = leafLookup[slot]) != 0) {
            final int leaf = entry - 1;
            final byte[] leafName = leafNames[leaf];
            if (schema.getLeafParent(leaf) == parent && Arrays.equals(leafName, 0, leafName.length, name, offset, offset + length)) {
                return leaf;
            }
            slot = (slot + 1) & mask;
        }
        return declareField(new String(name, offset, length, StandardCharsets.UTF_8));
    }

    /** Writes a long into the current row of the declared field {@code field}. */
    public void longField(int field, long value) {
        rowColumn(field).setLong(docCount, value);
    }

    public void doubleField(int field, double value) {
        rowColumn(field).setDouble(docCount, value);
    }

    public void booleanField(int field, boolean value) {
        rowColumn(field).setBoolean(docCount, value);
    }

    public void stringField(int field, byte[] bytes, int offset, int length) {
        rowColumn(field).setString(docCount, bytes, offset, length);
    }

    /**
     * Writes the same long into rows {@code [row, row + count)} of the declared field {@code field}. The rows may already be
     * finished, but the field must not have been written in any of them, nor in a later row.
     */
    public void repeatedLongField(int field, int row, int count, long value) {
        rangeColumn(field, row, count).setLongRepeated(row, value, count);
    }

    public void repeatedDoubleField(int field, int row, int count, double value) {
        rangeColumn(field, row, count).setDoubleRepeated(row, value, count);
    }

    public void repeatedBooleanField(int field, int row, int count, boolean value) {
        rangeColumn(field, row, count).setBooleanRepeated(row, value, count);
    }

    public void repeatedStringField(int field, int row, int count, byte[] bytes, int offset, int length) {
        rangeColumn(field, row, count).setStringRepeated(row, bytes, offset, length, count);
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

    /** The column builder of {@code field} for a write to the current row, rejecting a second write to it within the row. */
    private EscfColumnBuilder rowColumn(int field) {
        if (writtenInRow[field] == docCount + 1) {
            throw new IllegalArgumentException("Duplicate field [" + schema.getLeafName(field) + "]");
        }
        writtenInRow[field] = docCount + 1;
        return builders.get(field);
    }

    /** The column builder of {@code field} for a write to rows {@code [row, row + count)}. */
    private EscfColumnBuilder rangeColumn(int field, int row, int count) {
        if (count <= 0 || row < 0 || row + count > docCount + 1) {
            throw new IllegalArgumentException("rows [" + row + ", " + (row + count) + ") are not within the batch");
        }
        if (writtenInRow[field] >= row + 1) {
            throw new IllegalArgumentException(
                "Duplicate field [" + schema.getLeafName(field) + "]: already written in row " + (writtenInRow[field] - 1)
            );
        }
        writtenInRow[field] = row + count;
        return builders.get(field);
    }

    /** Makes sure the per-leaf tables and the builder list cover {@code leaf}. */
    private void ensureLeaf(int leaf) {
        if (leaf >= writtenInRow.length) {
            int capacity = Math.max(leaf + 1, writtenInRow.length * 2);
            writtenInRow = Arrays.copyOf(writtenInRow, capacity);
            leafNames = Arrays.copyOf(leafNames, capacity);
        }
        // Rows before the first one a column is written in are absent: the builder fills them in lazily.
        while (builders.size() <= leaf) {
            builders.add(new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler, dictionaryStrings));
        }
    }

    private void insertLeafLookup(int leaf) {
        if ((leafLookupSize + 1) * 2 > leafLookup.length) {
            int[] old = leafLookup;
            leafLookup = new int[old.length * 2];
            for (int entry : old) {
                if (entry != 0) {
                    insertLeafLookupEntry(entry - 1);
                }
            }
        }
        insertLeafLookupEntry(leaf);
        leafLookupSize++;
    }

    private void insertLeafLookupEntry(int leaf) {
        final byte[] name = leafNames[leaf];
        final int mask = leafLookup.length - 1;
        int slot = StringHelper.murmurhash3_x86_32(name, 0, name.length, schema.getLeafParent(leaf)) & mask;
        while (leafLookup[slot] != 0) {
            slot = (slot + 1) & mask;
        }
        leafLookup[slot] = leaf + 1;
    }
}
