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
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * An ESCF STRING column stored as a dictionary of distinct values plus one unsigned 16-bit ordinal per document.
 *
 * <p>Its logical kind is {@link EscfColumnKind#STRING}, so it reads exactly like an {@link EscfStringColumn}. It exists because
 * columns such as OTLP resource and attribute strings repeat the same few values on every row: the dictionary holds each value once
 * and a row costs two bytes however long the value is. A column with more distinct values than an ordinal can address
 * ({@link EscfColumnData#MAX_DICTIONARY_SIZE}) is never built in this form.
 *
 * <p>Slices share the dictionary and window the ordinal array, so slicing stays O(1) apart from the validity window.
 */
final class EscfDictStringColumn extends EscfColumn {

    private final char[] ordinals;
    private final int ordinalBase;
    /** {@code dictionarySize + 1} offsets into {@link #dictBytes}. */
    private final int[] dictOffsets;
    private final BytesReference dictData;
    private final byte[] dictBytes;
    private final int dictBase;
    private EscfColumnData plain;

    /**
     * @param ordinals    ordinals of the documents {@code [ordinalBase, ordinalBase + docCount)}
     * @param dictOffsets {@code dictionarySize + 1} offsets into {@code dictData}
     * @param dictData    the concatenated dictionary values
     */
    EscfDictStringColumn(int docCount, FixedBitSet validity, char[] ordinals, int ordinalBase, int[] dictOffsets, BytesReference dictData) {
        super(docCount, validity);
        assert ordinalBase >= 0 && ordinalBase + docCount <= ordinals.length : "ordinal window out of range";
        this.ordinals = ordinals;
        this.ordinalBase = ordinalBase;
        this.dictOffsets = dictOffsets;
        this.dictData = dictData;
        // The dictionary is addressed by random access, so resolve it to one contiguous array (no copy when it already is one).
        BytesRef dictRef = dictData.toBytesRef();
        this.dictBytes = dictRef.bytes;
        this.dictBase = dictRef.offset;
    }

    @Override
    public byte kind() {
        return EscfColumnKind.STRING;
    }

    @Override
    byte typeByteForPresent(int row) {
        return SourceValueType.STRING;
    }

    @Override
    public BytesRef getBinaryValue(int row) {
        if (isAbsent(row)) {
            return new BytesRef();
        }
        return entry(ordinals[ordinalBase + row], new BytesRef());
    }

    @Override
    public Text getStringValue(int row) {
        BytesRef ref = getBinaryValue(row);
        return new Text(new XContentString.UTF8Bytes(ref.bytes, ref.offset, ref.length));
    }

    @Override
    public ObjectTupleCursor<BytesRef> bytesRefCursor(boolean retainValues) {
        return new ObjectTupleCursor<>() {
            private final PresentDocIterator present = presentDocs();
            private final BytesRef shared = new BytesRef();
            private BytesRef current;

            @Override
            public int nextDoc() {
                int doc = present.nextDoc();
                if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                    current = entry(ordinals[ordinalBase + doc], retainValues ? new BytesRef() : shared);
                }
                return doc;
            }

            @Override
            public BytesRef value() {
                return current;
            }
        };
    }

    /** The number of distinct values in the dictionary. */
    int dictionarySize() {
        return dictOffsets.length - 1;
    }

    /** The ordinal of document {@code row}, which must be present. */
    int ordinal(int row) {
        return ordinals[ordinalBase + row];
    }

    /** The length in bytes of the longest dictionary entry. */
    int maxEntryLength() {
        int max = 0;
        for (int i = 0; i < dictionarySize(); i++) {
            max = Math.max(max, dictOffsets[i + 1] - dictOffsets[i]);
        }
        return max;
    }

    /** The dictionary as one {@link BytesRef} per entry, indexed by ordinal. The entries are views over the shared dictionary bytes. */
    List<BytesRef> dictionaryEntries() {
        List<BytesRef> entries = new ArrayList<>(dictionarySize());
        for (int ordinal = 0; ordinal < dictionarySize(); ordinal++) {
            entries.add(entry(ordinal, new BytesRef()));
        }
        return entries;
    }

    /** The total size in bytes of all dictionary entries. */
    int dictionaryBytes() {
        return dictOffsets[dictionarySize()] - dictOffsets[0];
    }

    /** Points {@code into} at dictionary entry {@code ordinal} and returns it. */
    BytesRef entry(int ordinal, BytesRef into) {
        int start = dictOffsets[ordinal];
        into.bytes = dictBytes;
        into.offset = dictBase + start;
        into.length = dictOffsets[ordinal + 1] - start;
        return into;
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfDictStringColumn(count, windowValidity(validity, from, count), ordinals, ordinalBase + from, dictOffsets, dictData);
    }

    @Override
    EscfColumnData toColumnData() {
        char[] windowOrdinals = ordinalBase == 0 && ordinals.length == docCount
            ? ordinals
            : Arrays.copyOfRange(ordinals, ordinalBase, ordinalBase + docCount);
        return EscfColumnData.ofDictString(docCount, validity, dictOffsets, dictData, windowOrdinals);
    }

    @Override
    EscfColumnData toPlainColumnData() {
        if (plain == null) {
            plain = materialize();
        }
        return plain;
    }

    /** Expands the dictionary into the plain one-offset-per-document layout. */
    private EscfColumnData materialize() {
        int[] offsets = new int[docCount + 1];
        long total = 0;
        for (int row = 0; row < docCount; row++) {
            if (isPresent(row)) {
                int ordinal = ordinals[ordinalBase + row];
                total += dictOffsets[ordinal + 1] - dictOffsets[ordinal];
            }
            if (total > Integer.MAX_VALUE) {
                throw new IllegalStateException("string column of [" + docCount + "] documents exceeds 2GB when expanded");
            }
            offsets[row + 1] = (int) total;
        }
        byte[] out = new byte[(int) total];
        for (int row = 0; row < docCount; row++) {
            if (isPresent(row)) {
                int ordinal = ordinals[ordinalBase + row];
                int start = dictOffsets[ordinal];
                System.arraycopy(dictBytes, dictBase + start, out, offsets[row], dictOffsets[ordinal + 1] - start);
            }
        }
        return EscfColumnData.ofVarWidth(EscfColumnKind.STRING, docCount, validity, offsets, new BytesArray(out));
    }
}
