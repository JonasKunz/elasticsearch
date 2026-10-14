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
 *   <li>{@code ordinals} — one unsigned 16-bit dictionary ordinal per document; only set for a
 *       <b>dictionary-encoded STRING</b> column (see {@link #ofDictString}), {@code null} for every other column. In that
 *       layout {@code offsets} has {@code dictionarySize + 1} entries and {@code data} holds the dictionary's
 *       concatenated values, instead of the per-document layout described above. The ordinal of an absent
 *       document is {@code 0} and meaningless.</li>
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
    char[] ordinals
) implements Releasable {

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
     * STRING, dictionary-encoded: the distinct values live once in the dictionary and every document holds a 16-bit
     * ordinal into it. The column's logical kind is still {@link EscfColumnKind#STRING}; only its physical layout differs.
     *
     * <p>Batches of OTLP metrics repeat the same resource, scope and attribute strings on every row, which this layout stores
     * once per batch instead of once per row. A column with more than {@link #MAX_DICTIONARY_SIZE} distinct values cannot be
     * represented and stays a plain {@link #ofVarWidth} column.
     *
     * @param dictionaryOffsets {@code dictionarySize + 1} byte offsets into {@code dictionaryData}, one value per entry
     * @param dictionaryData    the concatenated dictionary values
     * @param ordinals          one ordinal per document, each below {@code dictionarySize} for present documents
     */
    static EscfColumnData ofDictString(
        int docCount,
        FixedBitSet validity,
        int[] dictionaryOffsets,
        BytesReference dictionaryData,
        char[] ordinals
    ) {
        assert ordinals.length == docCount : "ordinals length " + ordinals.length + " != docCount " + docCount;
        assert dictionaryOffsets.length - 1 <= MAX_DICTIONARY_SIZE : "dictionary too large: " + (dictionaryOffsets.length - 1);
        return new EscfColumnData(EscfColumnKind.STRING, docCount, validity, null, null, dictionaryOffsets, dictionaryData, null, ordinals);
    }

    /** The most distinct values a dictionary-encoded column can hold: the number of values a 16-bit ordinal can address. */
    public static final int MAX_DICTIONARY_SIZE = 1 << 16;

    /** {@code true} for a dictionary-encoded STRING column (see {@link #ofDictString}). */
    public boolean isDictionaryEncoded() {
        return ordinals != null;
    }
}
