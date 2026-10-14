/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.xcontent.Text;

import java.util.Arrays;

/**
 * A run-length-encoded wrapper around an ordinary scalar ESCF column (LONG, DOUBLE, BOOL, STRING or BINARY).
 *
 * <p>The wrapped column, {@code runs}, holds one entry per run of consecutive equal documents, where consecutive absent documents
 * also form a run. A parallel array stores for each run how many documents it covers, minus one, as an unsigned byte, so a run
 * covers 1 to {@link EscfColumnData#MAX_RUN_LENGTH} documents and a longer stretch of equal documents is stored as several runs with
 * the same value. This pays off for columns whose values repeat in long stretches, such as the resource and scope fields of a batch
 * of OTLP data points, and lets a consumer that works per distinct value, rather than per document, skip the repeats.
 *
 * <p>Besides transparently serving every {@link EscfColumn} accessor, the wrapper exposes its runs ({@link #runs()},
 * {@link #runOf(int)}) for such consumers. {@link #kind()} is the kind of the wrapped values, so callers need not care.
 */
public final class EscfRleColumn extends EscfColumn {

    private final EscfColumn runs;
    /** {@code runLength - 1} of every run; has at least {@code runs.docCount} entries, only that many are used. */
    private final byte[] repetitions;
    /** Row at which run {@code i} starts; {@code runStart[runs.docCount]} is this column's {@code docCount}. */
    private final int[] runStart;

    /** The run the last {@link #runOf} lookup found, to make sequential access constant-time. */
    private int lastRun;
    private EscfColumnData plain;

    private EscfRleColumn(int docCount, FixedBitSet validity, EscfColumn runs, byte[] repetitions, int[] runStart) {
        super(docCount, validity);
        this.runs = runs;
        this.repetitions = repetitions;
        this.runStart = runStart;
    }

    /** Wraps {@code runs}, whose {@code i}-th entry covers {@code (repetitions[i] & 0xFF) + 1} documents. */
    static EscfRleColumn of(EscfColumn runs, byte[] repetitions) {
        assert runs instanceof EscfRleColumn == false && runs.kind() != EscfColumnKind.ARRAY && runs.kind() != EscfColumnKind.UNION
            : "cannot run-length-encode a " + EscfColumnKind.name(runs.kind()) + " column";
        int runCount = runs.docCount();
        int[] starts = new int[runCount + 1];
        int rows = 0;
        for (int i = 0; i < runCount; i++) {
            starts[i] = rows;
            rows += (repetitions[i] & 0xFF) + 1;
        }
        starts[runCount] = rows;

        FixedBitSet validity = null;
        if (runs.isDense() == false) {
            validity = new FixedBitSet(rows);
            for (int i = 0; i < runCount; i++) {
                if (runs.isPresent(i)) {
                    validity.set(starts[i], starts[i + 1]);
                }
            }
            if (validity.cardinality() == rows) {
                validity = null;
            }
        }
        return new EscfRleColumn(rows, validity, runs, repetitions, starts);
    }

    @Override
    public byte kind() {
        return runs.kind();
    }

    /** The column holding one entry per run. Its documents are runs, not rows of this column. */
    public EscfColumn runs() {
        return runs;
    }

    /** The number of runs. */
    public int runCount() {
        return runs.docCount();
    }

    /** The index, in {@link #runs()}, of the run containing {@code row}. */
    public int runOf(int row) {
        assert row >= 0 && row < docCount : "row " + row + " out of [0, " + docCount + ")";
        int run = lastRun;
        if (row >= runStart[run] && row < runStart[run + 1]) {
            return run;
        }
        if (run + 1 < runCount() && row >= runStart[run + 1] && row < runStart[run + 2]) {
            return lastRun = run + 1;
        }
        int found = Arrays.binarySearch(runStart, 0, runCount() + 1, row);
        return lastRun = found >= 0 ? found : -found - 2;
    }

    /** The first row of run {@code run}. */
    public int runStart(int run) {
        return runStart[run];
    }

    /** The number of rows covered by run {@code run}. */
    public int runLength(int run) {
        return runStart[run + 1] - runStart[run];
    }

    @Override
    byte typeByteForPresent(int row) {
        return runs.typeByteForPresent(runOf(row));
    }

    @Override
    public boolean getBooleanValue(int row) {
        return runs.getBooleanValue(runOf(row));
    }

    @Override
    public long getLongValue(int row) {
        return runs.getLongValue(runOf(row));
    }

    @Override
    public double getDoubleValue(int row) {
        return runs.getDoubleValue(runOf(row));
    }

    @Override
    public Text getStringValue(int row) {
        return runs.getStringValue(runOf(row));
    }

    @Override
    public BytesRef getBinaryValue(int row) {
        return runs.getBinaryValue(runOf(row));
    }

    @Override
    public LongTupleCursor longCursor() {
        return new RepeatingLongCursor(runs.longCursor());
    }

    @Override
    public ObjectTupleCursor<BytesRef> bytesRefCursor(boolean retainValues) {
        return new RepeatingBytesRefCursor(runs.bytesRefCursor(retainValues));
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        if (count == 0) {
            return of(runs.sliceInternal(0, 0), new byte[0]);
        }
        int firstRun = runOf(from);
        int endRun = runOf(from + count - 1);
        int runCount = endRun - firstRun + 1;
        byte[] reps = Arrays.copyOfRange(repetitions, firstRun, endRun + 1);
        // trim the rows of the boundary runs that fall outside the slice
        int firstLength = runStart[firstRun + 1] - from;
        if (firstRun == endRun) {
            firstLength = count;
        }
        reps[0] = (byte) (firstLength - 1);
        if (runCount > 1) {
            reps[runCount - 1] = (byte) (from + count - runStart[endRun] - 1);
        }
        return of(runs.sliceInternal(firstRun, runCount), reps);
    }

    @Override
    EscfColumnData toColumnData() {
        int runCount = runCount();
        return EscfColumnData.ofRle(docCount, runs.toColumnData(), Arrays.copyOf(repetitions, runCount));
    }

    /** This column expanded to one entry per document, which is what consumers that cannot work on runs expect. */
    @Override
    synchronized EscfColumnData toPlainColumnData() {
        if (plain == null) {
            plain = expand();
        }
        return plain;
    }

    private EscfColumnData expand() {
        EscfColumnBuilder builder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT);
        builder.hintScalar(kind());
        for (int run = 0; run < runCount(); run++) {
            if (runs.isAbsent(run)) {
                continue; // the builder fills gaps with absents
            }
            int row = runStart[run];
            int length = runLength(run);
            switch (kind()) {
                case EscfColumnKind.LONG, EscfColumnKind.DOUBLE -> {
                    long bits = ((AbstractFixed64Column) runs).rawLong(run);
                    for (int i = 0; i < length; i++) {
                        builder.setRawFixed64(row + i, kind(), bits);
                    }
                }
                case EscfColumnKind.BOOL -> builder.setBooleanRepeated(row, runs.getBooleanValue(run), length);
                case EscfColumnKind.STRING, EscfColumnKind.BINARY -> {
                    BytesRef value = runs.getBinaryValue(run);
                    for (int i = 0; i < length; i++) {
                        if (kind() == EscfColumnKind.STRING) {
                            builder.setString(row + i, value.bytes, value.offset, value.length);
                        } else {
                            builder.setBinary(row + i, value);
                        }
                    }
                }
                default -> throw new IllegalStateException("cannot run-length-encode a " + EscfColumnKind.name(kind()) + " column");
            }
        }
        return builder.finish(docCount);
    }

    /** Yields each value of the run cursor once per document of its run. */
    private abstract class RepeatingCursor {
        private int doc = -1;
        private int runEnd;

        /** Moves to the next present run and returns its index, or {@link DocIdSetIterator#NO_MORE_DOCS}. */
        abstract int nextRun();

        public final int nextDoc() {
            if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                return doc;
            }
            if (doc + 1 < runEnd) {
                return ++doc;
            }
            int run = nextRun();
            if (run == DocIdSetIterator.NO_MORE_DOCS) {
                return doc = DocIdSetIterator.NO_MORE_DOCS;
            }
            runEnd = runStart[run + 1];
            return doc = runStart[run];
        }
    }

    private final class RepeatingLongCursor extends LongTupleCursor {
        private final LongTupleCursor runCursor;
        private final RepeatingCursor rows;

        RepeatingLongCursor(LongTupleCursor runCursor) {
            this.runCursor = runCursor;
            this.rows = new RepeatingCursor() {
                @Override
                int nextRun() {
                    return runCursor.nextDoc();
                }
            };
        }

        @Override
        public int nextDoc() {
            return rows.nextDoc();
        }

        @Override
        public long longValue() {
            return runCursor.longValue();
        }
    }

    private final class RepeatingBytesRefCursor extends ObjectTupleCursor<BytesRef> {
        private final ObjectTupleCursor<BytesRef> runCursor;
        private final RepeatingCursor rows;

        RepeatingBytesRefCursor(ObjectTupleCursor<BytesRef> runCursor) {
            this.runCursor = runCursor;
            this.rows = new RepeatingCursor() {
                @Override
                int nextRun() {
                    return runCursor.nextDoc();
                }
            };
        }

        @Override
        public int nextDoc() {
            return rows.nextDoc();
        }

        @Override
        public BytesRef value() {
            return runCursor.value();
        }
    }

    @Override
    public String toString() {
        return "EscfRleColumn[" + EscfColumnKind.name(kind()) + ", " + docCount + " rows, " + runCount() + " runs]";
    }
}
