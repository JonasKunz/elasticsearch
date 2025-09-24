/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;

class BucketBuffer {

    private long[] indices;
    private long[] counts;
    private int size;

    private long[] tempIndices;
    private long[] tempCounts;

    BucketBuffer(int capacity) {
        this.indices = new long[capacity];
        this.counts = new long[capacity];
        this.tempIndices = new long[capacity];
        this.tempCounts = new long[capacity];
        size = 0;
    }

    boolean isFull() {
        return size == indices.length;
    }

    boolean isEmpty() {
        return size == 0;
    }

    int getMaximumScale() {
        int maxScale = MAX_SCALE;
        int currentHeaderSlot = 0;
        while (currentHeaderSlot < size) {
            int numBuckets = getNegativeBucketCount(currentHeaderSlot) + getPositiveBucketCount(currentHeaderSlot);
            int bucketsStart = currentHeaderSlot + 1;
            int bucketsEnd = bucketsStart + numBuckets;
            int scale = getScale(currentHeaderSlot);
            for (int i = bucketsStart; i < bucketsEnd; i++) {
                maxScale = Math.min(maxScale, scale + ExponentialScaleUtils.getMaximumScaleIncrease(indices[i]));
            }
            currentHeaderSlot = bucketsEnd;
        }
        return maxScale;
    }

    void tryAppend(BucketIterator negativeBuckets, BucketIterator positiveBuckets) {
        assert negativeBuckets.scale() == positiveBuckets.scale();
        if (isFull()) {
            return;
        }
        int scale = negativeBuckets.scale();
        int headerSlot = size++;
        tryAppendBuckets(negativeBuckets);
        int numNegativeBucketsAdded = size - headerSlot - 1;
        tryAppendBuckets(positiveBuckets);
        int numPositiveBucketsAdded = size - headerSlot - 1 - numNegativeBucketsAdded;
        setHeader(indices, counts, headerSlot, scale, numNegativeBucketsAdded, numPositiveBucketsAdded);
    }

    void clear() {
        size = 0;
    }

    record MergeResult(CopyableBucketIterator negativeBuckets, CopyableBucketIterator positiveBuckets) {}

    private CopyableBucketIterator createIterator(int scale, int start, int end) {
        return new BucketArrayIterator(scale, counts, indices, start, end);
    }

    MergeResult mergeBufferedAndGet(int preferredScale) {
        if (isEmpty()) {
            CopyableBucketIterator emptyIterator = createIterator(MAX_SCALE, 0, 0);
            return new MergeResult(emptyIterator, emptyIterator);
        }
        while (containsSingleHistogram() == false) {
            mergePairwiseNeighbors(preferredScale);
        }
        int negBucketsStart = 1;
        int negBucketsEnd = negBucketsStart + getNegativeBucketCount(0);
        int posBucketsEnd = negBucketsEnd + getPositiveBucketCount(0);
        int scale = getScale(0);
        return new MergeResult(createIterator(scale, negBucketsStart, negBucketsEnd), createIterator(scale, negBucketsEnd, posBucketsEnd));
    }

    private void mergePairwiseNeighbors(int preferredScale) {
        int outputSize = 0;
        int currentHeaderSlot = 0;
        while (currentHeaderSlot < size) {
            int leftNegBucketsStart = currentHeaderSlot + 1;
            int leftNegBucketCount = getNegativeBucketCount(currentHeaderSlot);
            int leftNegBucketsEnd = leftNegBucketsStart + leftNegBucketCount;
            int leftPosBucketCount = getPositiveBucketCount(currentHeaderSlot);
            int leftPosBucketsEnd = leftNegBucketsEnd + leftPosBucketCount;
            int rightHeaderSlot = leftPosBucketsEnd;
            int leftScale = getScale(currentHeaderSlot);

            if (rightHeaderSlot >= size) {
                // no other histogram to merge with, copy as is and finish
                int outputHeaderSlot = outputSize++;
                setHeader(tempIndices, tempCounts, outputHeaderSlot, leftScale, leftNegBucketCount, leftPosBucketCount);
                outputSize = appendBucketsToTemp(createIterator(leftScale, leftNegBucketsStart, leftNegBucketsEnd), outputSize);
                outputSize = appendBucketsToTemp(createIterator(leftScale, leftNegBucketsEnd, leftPosBucketsEnd), outputSize);

                break;
            } else {
                int rightNegBucketsStart = rightHeaderSlot + 1;
                int rightNegBucketsEnd = rightNegBucketsStart + getNegativeBucketCount(rightHeaderSlot);
                int rightPosBucketsEnd = rightNegBucketsEnd + getPositiveBucketCount(rightHeaderSlot);

                int rightScale = getScale(rightHeaderSlot);

                int outputHeaderSlot = outputSize++;

                MergingBucketIterator negMerged = new MergingBucketIterator(
                    createIterator(leftScale, leftNegBucketsStart, leftNegBucketsEnd),
                    createIterator(rightScale, rightNegBucketsStart, rightNegBucketsEnd),
                    preferredScale
                );
                outputSize = appendBucketsToTemp(negMerged, outputSize);
                int numNegBuckets = outputSize - outputHeaderSlot - 1;

                MergingBucketIterator posMerged = new MergingBucketIterator(
                    createIterator(leftScale, leftNegBucketsEnd, leftPosBucketsEnd),
                    createIterator(rightScale, rightNegBucketsEnd, rightPosBucketsEnd),
                    preferredScale
                );
                outputSize = appendBucketsToTemp(posMerged, outputSize);
                int numPosBuckets = outputSize - outputHeaderSlot - 1 - numNegBuckets;
                setHeader(tempIndices, tempCounts, outputHeaderSlot, preferredScale, numNegBuckets, numPosBuckets);

                currentHeaderSlot = rightPosBucketsEnd;
            }
        }
        size = outputSize;
        swapIndicesAndCountsWithTemp();
    }

    private void swapIndicesAndCountsWithTemp() {
        long[] swap = indices;
        indices = tempIndices;
        tempIndices = swap;

        swap = counts;
        counts = tempCounts;
        tempCounts = swap;
    }

    private boolean containsSingleHistogram() {
        return 1 + getNegativeBucketCount(0) + getPositiveBucketCount(0) == size;
    }

    private void tryAppendBuckets(BucketIterator bucketIterator) {
        while (bucketIterator.hasNext() && isFull() == false) {
            indices[size] = bucketIterator.peekIndex();
            counts[size] = bucketIterator.peekCount();
            bucketIterator.advance();
            size++;
        }
    }

    private int appendBucketsToTemp(BucketIterator bucketIterator, int offset) {
        while (bucketIterator.hasNext()) {
            tempIndices[offset] = bucketIterator.peekIndex();
            tempCounts[offset] = bucketIterator.peekCount();
            bucketIterator.advance();
            offset++;
        }
        return offset;
    }

    private static void setHeader(long[] indices, long[] counts, int slot, int scale, int numNegativeBuckets, int numPositiveBuckets) {
        long packedCount = numNegativeBuckets | (((long) numPositiveBuckets) << 32);
        indices[slot] = scale;
        counts[slot] = packedCount;
    }

    private int getScale(int headerSlot) {
        return (int) indices[headerSlot];
    }

    private int getNegativeBucketCount(int headerSlot) {
        return (int) (counts[headerSlot] & 0xFFFFFFFFL);
    }

    private int getPositiveBucketCount(int headerSlot) {
        return (int) ((counts[headerSlot] >>> 32) & 0xFFFFFFFFL);
    }

}
