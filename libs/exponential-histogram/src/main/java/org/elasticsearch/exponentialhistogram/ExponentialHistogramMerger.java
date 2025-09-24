/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.function.DoubleBinaryOperator;

/**
 * Allows accumulating multiple {@link ExponentialHistogram} into a single one
 * while keeping the bucket count in the result below a given limit.
 */
public class ExponentialHistogramMerger implements Accountable, Releasable {

    private static final long BASE_SIZE = RamUsageEstimator.shallowSizeOfInstance(ExponentialHistogramMerger.class) + DownscaleStats.SIZE;

    // Our algorithm is not in-place, therefore we use two histograms and ping-pong between them
    @Nullable
    private FixedCapacityExponentialHistogram result;
    @Nullable
    private FixedCapacityExponentialHistogram buffer;

    private final BucketBuffer bucketBuffer;

    private final int bucketLimit;
    private final int maxScale;

    private final DownscaleStats downscaleStats;

    private final ExponentialHistogramCircuitBreaker circuitBreaker;
    private boolean closed = false;

    /**
     * Creates a new instance with the specified bucket limit.
     *
     * @param bucketLimit the maximum number of buckets the result histogram is allowed to have
     * @param circuitBreaker the circuit breaker to use to limit memory allocations
     */
    public static ExponentialHistogramMerger create(int bucketLimit, ExponentialHistogramCircuitBreaker circuitBreaker) {
        circuitBreaker.adjustBreaker(BASE_SIZE);
        return new ExponentialHistogramMerger(bucketLimit, circuitBreaker);
    }

    private ExponentialHistogramMerger(int bucketLimit, ExponentialHistogramCircuitBreaker circuitBreaker) {
        this(bucketLimit, ExponentialHistogram.MAX_SCALE, circuitBreaker);
    }

    private ExponentialHistogramMerger(int bucketLimit, int maxScale, ExponentialHistogramCircuitBreaker circuitBreaker) {
        this.bucketLimit = bucketLimit;
        this.maxScale = maxScale;
        this.circuitBreaker = circuitBreaker;
        downscaleStats = new DownscaleStats();
        // TODO: adjust circuit breaker
        bucketBuffer = new BucketBuffer(bucketLimit + 1);
    }

    // Only intended for testing, using this in production means an unnecessary reduction of precision
    static ExponentialHistogramMerger createForTesting(int bucketLimit, int maxScale, ExponentialHistogramCircuitBreaker circuitBreaker) {
        circuitBreaker.adjustBreaker(BASE_SIZE);
        return new ExponentialHistogramMerger(bucketLimit, maxScale, circuitBreaker);
    }

    @Override
    public void close() {
        if (closed) {
            assert false : "ExponentialHistogramMerger closed multiple times";
        } else {
            closed = true;
            if (result != null) {
                result.close();
                result = null;
            }
            if (buffer != null) {
                buffer.close();
                buffer = null;
            }
            circuitBreaker.adjustBreaker(-BASE_SIZE);
        }
    }

    @Override
    public long ramBytesUsed() {
        long size = BASE_SIZE;
        if (result != null) {
            size += result.ramBytesUsed();
        }
        if (buffer != null) {
            size += buffer.ramBytesUsed();
        }
        return size;
    }

    /**
     * Returns the merged histogram and clears this merger.
     * The caller takes ownership of the returned histogram and must ensure that {@link #close()} is called.
     *
     * @return the merged histogram
     */
    public ReleasableExponentialHistogram getAndClear() {
        assert closed == false : "ExponentialHistogramMerger already closed";
        mergeBufferedBucketsIntoResult();
        ReleasableExponentialHistogram retVal = (result == null) ? ReleasableExponentialHistogram.empty() : result;
        result = null;
        return retVal;
    }

    // TODO(b/128622): this algorithm is very efficient if b has roughly as many buckets as a
    // However, if b is much smaller we still have to iterate over all buckets of a which is very wasteful.
    // This can be optimized by buffering multiple histograms to accumulate first,
    // then in O(log(n)) turn them into a single, merged histogram.
    // (n is the number of buffered buckets)

    /**
     * Merges the given histogram into the current result.
     *
     * @param toAdd the histogram to merge
     */
    public void add(ExponentialHistogram toAdd) {
        ExponentialHistogram a = result == null ? ExponentialHistogram.empty() : result;
        ExponentialHistogram b = toAdd;

        CopyableBucketIterator posBucketsA = a.positiveBuckets().iterator();
        CopyableBucketIterator negBucketsA = a.negativeBuckets().iterator();
        CopyableBucketIterator posBucketsB = b.positiveBuckets().iterator();
        CopyableBucketIterator negBucketsB = b.negativeBuckets().iterator();

        ZeroBucket zeroBucket = a.zeroBucket().merge(b.zeroBucket());
        zeroBucket = zeroBucket.collapseOverlappingBucketsForAll(posBucketsA, negBucketsA, posBucketsB, negBucketsB);

        boolean maybeCollapsedBucketsIntoZeroBucket = zeroBucket.compareZeroThreshold(a.zeroBucket()) != 0;
        if (maybeCollapsedBucketsIntoZeroBucket && bucketBuffer.isEmpty() == false) {
            // zero threshold has changed and we have buffered buckets, merge them first and then redo the collapsing
            mergeWithBufferedBucketsIntoResult(negBucketsA, posBucketsA);
            negBucketsA = result.negativeBuckets().iterator();
            posBucketsA = result.positiveBuckets().iterator();
            zeroBucket = zeroBucket.collapseOverlappingBucketsForAll(posBucketsA, negBucketsA, posBucketsB, negBucketsB);
        }

        double sum = a.sum() + b.sum();
        double min = nanAwareAggregate(a.min(), b.min(), Math::min);
        double max = nanAwareAggregate(a.max(), b.max(), Math::max);

        while (negBucketsB.hasNext() || posBucketsB.hasNext()) {
            bucketBuffer.tryAppend(negBucketsB, posBucketsB);
            if (bucketBuffer.isFull()) {
                mergeWithBufferedBucketsIntoResult(negBucketsA, posBucketsA);
                negBucketsA = result.negativeBuckets().iterator();
                posBucketsA = result.positiveBuckets().iterator();
                maybeCollapsedBucketsIntoZeroBucket = false;
            }
        }

        if (maybeCollapsedBucketsIntoZeroBucket) {
            mergeWithBufferedBucketsIntoResult(negBucketsA, posBucketsA);
        }

        if (result == null) {
            result = FixedCapacityExponentialHistogram.create(bucketLimit, circuitBreaker);
        }
        result.setZeroBucket(zeroBucket);
        result.setSum(sum);
        result.setMin(min);
        result.setMax(max);
    }

    private void mergeBufferedBucketsIntoResult() {
        if (bucketBuffer.isEmpty()) {
            return;
        }
        assert result != null : "result must be populated because min/max/sum was set";
        double min = result.min();
        double max = result.max();
        double sum = result.sum();
        ZeroBucket zeroBucket = result.zeroBucket();
        mergeWithBufferedBucketsIntoResult(result.negativeBuckets().iterator(), result.positiveBuckets().iterator());
        result.setMin(min);
        result.setMax(max);
        result.setSum(sum);
        result.setZeroBucket(zeroBucket);
    }

    private void mergeWithBufferedBucketsIntoResult(CopyableBucketIterator negBucketsA, CopyableBucketIterator posBucketsA) {
        if (buffer == null) {
            buffer = FixedCapacityExponentialHistogram.create(bucketLimit, circuitBreaker);
        }
        int targetScale = Math.min(Math.min(maxScale, negBucketsA.scale()), bucketBuffer.getMaximumScale());

        // Now we are sure that everything fits numerically into targetScale.
        // However, we might exceed our limit for the total number of buckets.
        // Therefore, we try the merge optimistically. If we fail, we reduce the target scale to make everything fit.

        BucketBuffer.MergeResult bufferIterators = bucketBuffer.mergeBufferedAndGet(targetScale);
        MergingBucketIterator positiveMerged = new MergingBucketIterator(
            posBucketsA.copy(),
            bufferIterators.positiveBuckets().copy(),
            targetScale
        );
        MergingBucketIterator negativeMerged = new MergingBucketIterator(
            negBucketsA.copy(),
            bufferIterators.negativeBuckets().copy(),
            targetScale
        );

        buffer.resetBuckets(targetScale);
        downscaleStats.reset();
        int overflowCount = putBuckets(buffer, negativeMerged, false, downscaleStats);
        overflowCount += putBuckets(buffer, positiveMerged, true, downscaleStats);

        if (overflowCount > 0) {
            // UDD-sketch approach: decrease the scale and retry.
            int reduction = downscaleStats.getRequiredScaleReductionToReduceBucketCountBy(overflowCount);
            targetScale -= reduction;
            buffer.resetBuckets(targetScale);
            positiveMerged = new MergingBucketIterator(posBucketsA, bufferIterators.positiveBuckets(), targetScale);
            negativeMerged = new MergingBucketIterator(negBucketsA, bufferIterators.negativeBuckets(), targetScale);
            overflowCount = putBuckets(buffer, negativeMerged, false, null);
            overflowCount += putBuckets(buffer, positiveMerged, true, null);

            assert overflowCount == 0 : "Should never happen, the histogram should have had enough space";
        }
        bucketBuffer.clear();
        FixedCapacityExponentialHistogram temp = result;
        result = buffer;
        buffer = temp;
    }

    private static int putBuckets(
        FixedCapacityExponentialHistogram output,
        BucketIterator buckets,
        boolean isPositive,
        DownscaleStats downscaleStats
    ) {
        boolean collectDownScaleStatsOnNext = false;
        long prevIndex = 0;
        int overflowCount = 0;
        while (buckets.hasNext()) {
            long idx = buckets.peekIndex();
            if (collectDownScaleStatsOnNext) {
                downscaleStats.add(prevIndex, idx);
            } else {
                collectDownScaleStatsOnNext = downscaleStats != null;
            }

            if (output.tryAddBucket(idx, buckets.peekCount(), isPositive) == false) {
                overflowCount++;
            }

            prevIndex = idx;
            buckets.advance();
        }
        return overflowCount;
    }

    private static double nanAwareAggregate(double a, double b, DoubleBinaryOperator aggregator) {
        if (Double.isNaN(a)) {
            return b;
        }
        if (Double.isNaN(b)) {
            return a;
        }
        return aggregator.applyAsDouble(a, b);
    }

}
