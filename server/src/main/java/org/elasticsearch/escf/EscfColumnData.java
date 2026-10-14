/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * A single ESCF column held in its <b>native</b> in-memory representation — the metadata factors are
 * live arrays/bitsets rather than pre-serialized bytes
 * <ul>
 *   <li>{@code validity} — Validity bitset (bit set = present); {@code null} when every document is present (dense).</li>
 *   <li>{@code values} — the BOOL value bitset (bit set = {@code true}); {@code null} for every other kind.</li>
 *   <li>{@code typeVector} — one {@link SourceValueType} byte per document as a windowed {@link BytesRef};
 *       {@code null} for kinds whose per-document type is implied by {@link #kind} (everything except UNION).</li>
 *   <li>{@code offsets} — {@code (docCount + 1)} entries; {@code null} for fixed-width kinds (LONG, DOUBLE) and BOOL.
 *       For STRING/BINARY/UNION these are byte offsets into {@code data}; for ARRAY they are per-row element-range
 *       offsets into {@code child}.</li>
 *   <li>{@code data} — the recycler-backed value payload; {@code null} for BOOL (values live in {@code values})
 *       and for ARRAY (the payload lives in {@code child}).</li>
 *   <li>{@code child} — the dense primitive sub-column for ARRAY (itself a native
 *       {@link EscfColumnData} of kind LONG, DOUBLE, or STRING); {@code null} for every other kind.
 *       Kept native rather than pre-serialized so the "native in-memory" invariant above also holds for
 *       ARRAY; it is only flattened to {@code child_kind(1) | child_values} bytes at the wire boundary in
 *       {@link EscfBatch}.</li>
 *   <li>{@code repetitions} — for a run-length-encoded column only (see {@link #ofRle}): one unsigned byte per run holding
 *       {@code runLength - 1}. Such a column's {@code docCount} is the number of logical documents, its {@code kind} is the kind of
 *       the values, and every other factor is {@code null} except {@code child}, which holds the column of the runs.</li>
 * </ul>
 */
public record EscfColumnData(
    byte kind,
    int docCount,
    FixedBitSet validity,
    FixedBitSet values,
    BytesRef typeVector,
    int[] offsets,
    BytesReference data,
    EscfColumnData child,
    byte[] repetitions
) implements Releasable {

    /** The most documents a single run of a run-length-encoded column can cover: its repetitions are stored in one unsigned byte. */
    public static final int MAX_RUN_LENGTH = 256;

    /**
     * Releases the recycler-backed buffers this column owns: its own {@code data} payload (when it is
     * {@link Releasable}) and, for ARRAY, its nested {@code child} column's buffers. Only columns built
     * natively by {@link EscfColumnBuilder} own releasable buffers; columns reconstructed from a
     * serialized batch hold non-releasable slices, for which this is a no-op.
     */
    @Override
    public void close() {
        Releasables.close(data instanceof Releasable releasable ? releasable : null, child);
    }

    /** LONG or DOUBLE: a dense 8-byte-per-document value payload; no offsets or type vector. */
    static EscfColumnData ofFixed64(byte kind, int docCount, FixedBitSet validity, BytesReference data) {
        return new EscfColumnData(kind, docCount, validity, null, null, null, data, null, null);
    }

    /** BOOL: the value bitset directly; no byte payload. */
    static EscfColumnData ofBool(int docCount, FixedBitSet validity, FixedBitSet values) {
        return new EscfColumnData(EscfColumnKind.BOOL, docCount, validity, values, null, null, null, null, null);
    }

    /** STRING or BINARY: an offset vector plus a dense byte payload. */
    static EscfColumnData ofVarWidth(byte kind, int docCount, FixedBitSet validity, int[] offsets, BytesReference data) {
        return new EscfColumnData(kind, docCount, validity, null, null, offsets, data, null, null);
    }

    /** ARRAY: per-row element-range offsets over a native primitive {@code child} sub-column. */
    static EscfColumnData ofArray(int docCount, FixedBitSet validity, int[] offsets, EscfColumnData child) {
        return new EscfColumnData(EscfColumnKind.ARRAY, docCount, validity, null, null, offsets, null, child, null);
    }

    /** UNION: a per-document type vector, an offset vector, and a dense value payload. */
    static EscfColumnData ofUnion(int docCount, FixedBitSet validity, BytesRef typeVector, int[] offsets, BytesReference data) {
        return new EscfColumnData(EscfColumnKind.UNION, docCount, validity, null, typeVector, offsets, data, null, null);
    }

    /**
     * A run-length-encoded column: {@code runs} holds one value (or absence) per run of consecutive equal documents, and
     * {@code repetitions[i] + 1} is the length of run {@code i}, so a run covers between 1 and {@link #MAX_RUN_LENGTH} documents.
     * A longer stretch of equal documents is stored as several consecutive runs with the same value. {@code runs} must be a LONG,
     * DOUBLE, BOOL, STRING or BINARY column.
     *
     * @param docCount    the number of logical documents, the sum of the run lengths
     * @param repetitions at least {@code runs.docCount()} entries; the first {@code runs.docCount()} are used
     */
    static EscfColumnData ofRle(int docCount, EscfColumnData runs, byte[] repetitions) {
        assert runs.isRunLengthEncoded() == false && runs.kind() != EscfColumnKind.ARRAY && runs.kind() != EscfColumnKind.UNION
            : "cannot run-length-encode a " + EscfColumnKind.name(runs.kind()) + " column";
        assert repetitions.length >= runs.docCount();
        assert sumOfRunLengths(repetitions, runs.docCount()) == docCount : "run lengths do not add up to " + docCount;
        return new EscfColumnData(runs.kind(), docCount, null, null, null, null, null, runs, repetitions);
    }

    /** Whether this column is a run-length-encoded wrapper around the column of its runs (see {@link #ofRle}). */
    public boolean isRunLengthEncoded() {
        return repetitions != null;
    }

    /** The number of runs of a run-length-encoded column. */
    int runCount() {
        assert isRunLengthEncoded();
        return child.docCount();
    }

    private static long sumOfRunLengths(byte[] repetitions, int runs) {
        long total = 0;
        for (int i = 0; i < runs; i++) {
            total += (repetitions[i] & 0xFF) + 1;
        }
        return total;
    }
}
