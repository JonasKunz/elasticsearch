/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import com.google.protobuf.ByteString;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Columnar counterpart to {@link MetricDocumentBuilder}: writes OTel metric data points directly
 * into an {@link OtlpMetricsBatchBuilder} without going through an intermediate XContent representation.
 *
 * <p>Unlike {@link MetricDocumentBuilder}, which writes one document per group of data points that share
 * resource, scope, attributes and timestamp, this class writes <b>one row per data point</b>. Grouping data points
 * rarely combines more than one metric into a document, yet finding the groups costs a tsid hash and several map
 * lookups per data point; the columnar path therefore skips it and the caller feeds data points straight from the
 * OTLP request. The consequence is that {@code _metric_names_hash} (a dimension) is always the hash of the single
 * metric name of the row, so for a series that the document path would have grouped with other metrics the
 * resulting {@code _tsid} differs between the two paths. For data points that are not grouped with others, both
 * paths produce identical {@code _tsid}, {@code _id} and {@code _source}.
 *
 * <p>The method {@link #buildRows} takes the whole parsed {@link ExportMetricsServiceRequest} and is optimistic: it
 * starts writing rows straight away and returns {@code null} as soon as it encounters something the columnar path
 * cannot express — a metric that is not a gauge or sum, a data point carrying exemplars while exemplar ingestion is
 * enabled, a non-scalar attribute value (ARRAY/KVLIST/BYTES), duplicate attribute keys (which
 * {@link OtlpMetricsBatchBuilder} rejects) or data points that target more than one data stream. There is deliberately no
 * up-front eligibility scan: the common case is that everything is supported, and a separate pass over all
 * attributes and data points would cost as much as writing them. Callers must treat a {@code null} return as a
 * signal to fall back to the {@link MetricDocumentBuilder} XContent path and discard the whole batch: values are
 * written straight into the column builders, so a partially written row stays in the batch as an aborted row.
 *
 * <p>The field layout emitted by this class is identical to that of {@link MetricDocumentBuilder}:
 * same field names, same nesting depth, same ordering. The {@code OTLPMetricsEscfComparisonRestIT}
 * integration test guards this contract by comparing {@code _id}, {@code _tsid}, and {@code _source}
 * for each document across the two paths.
 */
public final class MetricColumnarBuilder {

    private final MappingHints defaultMappingHints;
    private final boolean exemplarIngestionEnabled;
    private final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);
    // Data points of one metric arrive back to back, so remembering the last metric name avoids rehashing it.
    private String lastMetricName;
    private String lastMetricNamesHash;

    /**
     * @param defaultMappingHints      the mapping hints to use for data points that do not override them
     * @param exemplarIngestionEnabled whether exemplars are ingested as separate documents. Exemplar documents are
     *                                 only produced by the XContent path, so when this is {@code true} a data point
     *                                 that carries exemplars makes {@link #buildRows} return {@code null}.
     */
    public MetricColumnarBuilder(MappingHints defaultMappingHints, boolean exemplarIngestionEnabled) {
        this.defaultMappingHints = defaultMappingHints;
        this.exemplarIngestionEnabled = exemplarIngestionEnabled;
    }

    /**
     * The outcome of a successful {@link #buildRows} call.
     *
     * @param target         the single data stream all rows target; {@code null} if the request had no data points
     * @param rowRequests    one {@link IndexRequest} per batch row, in row order, carrying the per-row metadata
     *                       (dynamic templates and time-series timestamp) but no source yet
     * @param timestampsNanos the distinct, adjacent data point timestamps of the rows, used to determine which
     *                       backing indices the rows will be written to
     */
    public record Rows(@Nullable String target, List<IndexRequest> rowRequests, long[] timestampsNanos) {}

    /**
     * Writes every data point of {@code request} as one row of {@code batchBuilder}, in request order.
     *
     * <p>Returns {@code null} if the request cannot be written columnar (see the class documentation). In that case
     * the caller must discard {@code batchBuilder} and its contents, since a partially written row may remain.
     *
     * @param batchBuilder    the batch builder receiving the rows
     * @param request         the parsed OTLP request
     * @param targetSupported evaluated once, with the name of the first data point's target data stream, so that
     *                        streams that can never take the columnar path fail before any row is written
     * @return the rows' metadata, or {@code null} if the caller must fall back to the XContent path
     */
    @Nullable
    public Rows buildRows(OtlpMetricsBatchBuilder batchBuilder, ExportMetricsServiceRequest request, Predicate<String> targetSupported) {
        String target = null;
        final int firstRow = batchBuilder.docCount();
        List<IndexRequest> rowRequests = new ArrayList<>();
        long[] timestamps = new long[16];
        int numTimestamps = 0;

        List<ResourceMetrics> resourceMetricsList = request.getResourceMetricsList();
        for (int i = 0; i < resourceMetricsList.size(); i++) {
            ResourceMetrics resourceMetrics = resourceMetricsList.get(i);
            Resource resource = resourceMetrics.getResource();
            List<ScopeMetrics> scopeMetricsList = resourceMetrics.getScopeMetricsList();
            for (int j = 0; j < scopeMetricsList.size(); j++) {
                ScopeMetrics scopeMetrics = scopeMetricsList.get(j);
                InstrumentationScope scope = scopeMetrics.getScope();
                String scopeRoutingDataset = TargetIndex.extractScopeRoutingDataset(scope);
                List<Metric> metrics = scopeMetrics.getMetricsList();
                // All data points of a scope share its resource and scope fields, so they form one block: those fields are
                // written once, by the first row, instead of once per data point.
                int scopeDataPoints = 0;
                for (int k = 0; k < metrics.size(); k++) {
                    List<NumberDataPoint> dataPoints = numberDataPoints(metrics.get(k));
                    if (dataPoints == null) {
                        return null;
                    }
                    scopeDataPoints += dataPoints.size();
                }
                if (scopeDataPoints == 0) {
                    continue;
                }
                batchBuilder.beginBlock(scopeDataPoints);
                boolean blockLeader = true;
                for (int k = 0; k < metrics.size(); k++) {
                    Metric metric = metrics.get(k);
                    List<NumberDataPoint> dataPoints = numberDataPoints(metric);
                    for (int l = 0; l < dataPoints.size(); l++) {
                        DataPoint dataPoint = new DataPoint.Number(dataPoints.get(l), metric);
                        TargetIndex targetIndex = TargetIndex.evaluate(
                            TargetIndex.TYPE_METRICS,
                            dataPoint.getAttributes(),
                            scopeRoutingDataset,
                            scope.getAttributesList(),
                            resource.getAttributesList()
                        );
                        if (target == null) {
                            target = targetIndex.index();
                            if (targetSupported.test(target) == false) {
                                return null;
                            }
                        } else if (target.equals(targetIndex.index()) == false) {
                            // a bulk request carries at most one pre-built batch per target
                            return null;
                        }

                        var dynamicTemplates = Maps.<String, String>newHashMapWithExpectedSize(1);
                        var dynamicTemplateParams = Maps.<String, Map<String, String>>newHashMapWithExpectedSize(1);
                        if (writeDataPoint(
                            batchBuilder,
                            resource,
                            resourceMetrics.getSchemaUrlBytes(),
                            scope,
                            scopeMetrics.getSchemaUrlBytes(),
                            dataPoint,
                            targetIndex,
                            blockLeader,
                            dynamicTemplates,
                            dynamicTemplateParams
                        ) == false) {
                            return null;
                        }
                        blockLeader = false;
                        int rowIndex = batchBuilder.finishRow();
                        assert rowIndex == firstRow + rowRequests.size() : "rows are written one per request, in order";

                        long timestampNanos = dataPoint.getTimestampUnixNano();
                        if (numTimestamps == 0 || timestamps[numTimestamps - 1] != timestampNanos) {
                            timestamps = ArrayUtil.grow(timestamps, numTimestamps + 1);
                            timestamps[numTimestamps++] = timestampNanos;
                        }
                        rowRequests.add(
                            new IndexRequest(target).opType(DocWriteRequest.OpType.CREATE)
                                .setRequireDataStream(true)
                                .setIncludeSourceOnError(false)
                                .setDynamicTemplates(dynamicTemplates)
                                .setDynamicTemplateParams(dynamicTemplateParams)
                                .setTimeSeriesTimestamp(
                                    DataStream.getCanonicalTimestampBound(
                                        Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(timestampNanos))
                                    )
                                )
                        );
                    }
                }
            }
        }
        return new Rows(target, rowRequests, Arrays.copyOf(timestamps, numTimestamps));
    }

    /** The data points of a gauge or sum, or {@code null} for a metric that cannot be expressed as scalar columns. */
    @Nullable
    private static List<NumberDataPoint> numberDataPoints(Metric metric) {
        return switch (metric.getDataCase()) {
            case SUM -> metric.getSum().getDataPointsList();
            case GAUGE -> metric.getGauge().getDataPointsList();
            // not expressible as scalar columns, or not a supported metric type at all
            case HISTOGRAM, EXPONENTIAL_HISTOGRAM, SUMMARY, DATA_NOT_SET -> null;
        };
    }

    /**
     * Writes a single data point into the next row of {@code batchBuilder} and, on success, finishes nothing: the
     * caller calls {@link OtlpMetricsBatchBuilder#finishRow()}. On failure this method has either written nothing (the data
     * point was rejected up front) or has called {@link OtlpMetricsBatchBuilder#abortRow()}, which leaves an aborted row
     * in the batch, so callers must discard the whole batch.
     *
     * @return {@code true} if the row was written successfully
     */
    private boolean writeDataPoint(
        OtlpMetricsBatchBuilder batchBuilder,
        Resource resource,
        ByteString resourceSchemaUrl,
        InstrumentationScope scope,
        ByteString scopeSchemaUrl,
        DataPoint dataPoint,
        TargetIndex targetIndex,
        boolean blockLeader,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {
        if (dataPoint.supportsColumnarValue() == false) {
            return false;
        }
        if (exemplarIngestionEnabled && dataPoint.getExemplars().isEmpty() == false) {
            return false;
        }
        batchBuilder.beginRow();
        boolean written;
        try {
            written = writeRow(
                batchBuilder,
                resource,
                resourceSchemaUrl,
                scope,
                scopeSchemaUrl,
                dataPoint,
                targetIndex,
                blockLeader,
                dynamicTemplates,
                dynamicTemplateParams
            );
        } catch (IllegalArgumentException e) {
            // OtlpMetricsBatchBuilder throws IllegalArgumentException on duplicate field names, which OTel attribute
            // lists are allowed to contain.
            written = false;
        }
        if (written == false) {
            batchBuilder.abortRow();
        }
        return written;
    }

    private boolean writeRow(
        OtlpMetricsBatchBuilder batchBuilder,
        Resource resource,
        ByteString resourceSchemaUrl,
        InstrumentationScope scope,
        ByteString scopeSchemaUrl,
        DataPoint dataPoint,
        TargetIndex targetIndex,
        boolean blockLeader,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {
        batchBuilder.longField("@timestamp", TimeUnit.NANOSECONDS.toMillis(dataPoint.getTimestampUnixNano()));
        if (dataPoint.getStartTimestampUnixNano() != 0) {
            batchBuilder.longField("start_timestamp", TimeUnit.NANOSECONDS.toMillis(dataPoint.getStartTimestampUnixNano()));
        }

        // resource: the same for every row of the block, so only its first row writes it
        if (blockLeader && writeResource(batchBuilder, resource, resourceSchemaUrl) == false) {
            return false;
        }

        // data_stream
        writeDataStream(batchBuilder, targetIndex);

        // scope: likewise
        if (blockLeader && writeScope(batchBuilder, scope, scopeSchemaUrl) == false) {
            return false;
        }

        // datapoint attributes (top-level "attributes" object)
        if (writeAttributes(batchBuilder, dataPoint.getAttributes(), 0, false) == false) {
            return false;
        }

        String unit = dataPoint.getUnit();
        if (Strings.hasLength(unit)) {
            writeAsciiStringField(batchBuilder, MetricDocumentBuilder.UNIT_FIELD, unit);
        }

        String temporality = MetricDocumentBuilder.temporalityToString(dataPoint.getTemporality());
        if (temporality != null) {
            writeAsciiStringField(batchBuilder, MetricDocumentBuilder.TEMPORALITY_FIELD, temporality);
        }

        String metricName = dataPoint.getMetricName();
        writeAsciiStringField(batchBuilder, "_metric_names_hash", metricNamesHash(metricName));

        // metrics object
        batchBuilder.startObject("metrics");
        dataPoint.writeColumnarValue(batchBuilder, metricName);
        batchBuilder.endObject();

        MappingHints mappingHints = defaultMappingHints.withConfigFromAttributes(dataPoint.getAttributes());
        String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
        if (dynamicTemplate != null) {
            String metricFieldPath = "metrics." + metricName;
            dynamicTemplates.put(metricFieldPath, dynamicTemplate);
            if (unit != null && unit.isEmpty() == false) {
                dynamicTemplateParams.put(metricFieldPath, Map.of(MetricDocumentBuilder.UNIT_FIELD, unit));
            }
        }
        if (mappingHints.docCount()) {
            long docCount = dataPoint.getDocCount();
            if (docCount > 0) {
                batchBuilder.longField("_doc_count", docCount);
            }
        }
        return true;
    }

    /**
     * The {@code _metric_names_hash} dimension of a document holding exactly this one metric; computed the same way
     * as {@code DataPointGroupingContext.DataPointGroup#getMetricNamesHash} does for a single-metric group.
     */
    private String metricNamesHash(String metricName) {
        if (metricName.equals(lastMetricName) == false) {
            hasher.reset();
            hasher.addString(metricName);
            lastMetricNamesHash = Integer.toHexString(hasher.digestHash().hashCode());
            lastMetricName = metricName;
        }
        return lastMetricNamesHash;
    }

    // -------------------------------------------------------------------------
    // Resource / Scope / DataStream / Attributes
    // -------------------------------------------------------------------------

    private boolean writeResource(OtlpMetricsBatchBuilder batchBuilder, Resource resource, ByteString schemaUrl) {
        batchBuilder.startObject("resource");
        writeByteStringBlockFieldIfNotEmpty(batchBuilder, "schema_url", schemaUrl);
        if (writeAttributes(batchBuilder, resource.getAttributesList(), resource.getDroppedAttributesCount(), true) == false) {
            return false;
        }
        batchBuilder.endObject();
        return true;
    }

    private boolean writeScope(OtlpMetricsBatchBuilder batchBuilder, InstrumentationScope scope, ByteString schemaUrl) {
        batchBuilder.startObject("scope");
        writeByteStringBlockFieldIfNotEmpty(batchBuilder, "schema_url", schemaUrl);
        writeByteStringBlockFieldIfNotEmpty(batchBuilder, "name", scope.getNameBytes());
        writeByteStringBlockFieldIfNotEmpty(batchBuilder, "version", scope.getVersionBytes());
        if (writeAttributes(batchBuilder, scope.getAttributesList(), scope.getDroppedAttributesCount(), true) == false) {
            return false;
        }
        batchBuilder.endObject();
        return true;
    }

    private void writeDataStream(OtlpMetricsBatchBuilder batchBuilder, TargetIndex targetIndex) {
        if (targetIndex.isDataStream() == false) {
            return;
        }
        batchBuilder.startObject("data_stream");
        writeAsciiStringField(batchBuilder, "type", targetIndex.type());
        writeAsciiStringField(batchBuilder, "dataset", targetIndex.dataset());
        writeAsciiStringField(batchBuilder, "namespace", targetIndex.namespace());
        batchBuilder.endObject();
    }

    /**
     * Writes an attribute list as an {@code attributes} object. Returns {@code false} on the first attribute whose
     * value cannot be written as a scalar column; the row is then left half-written and must be discarded.
     *
     * @param block whether the attributes are the same for every row of the open block, so that they are written as block fields
     */
    private boolean writeAttributes(OtlpMetricsBatchBuilder batchBuilder, List<KeyValue> attributes, int droppedCount, boolean block) {
        if (droppedCount > 0) {
            if (block) {
                batchBuilder.blockLongField("dropped_attributes_count", droppedCount);
            } else {
                batchBuilder.longField("dropped_attributes_count", droppedCount);
            }
        }
        batchBuilder.startObject("attributes");
        for (int i = 0; i < attributes.size(); i++) {
            KeyValue kv = attributes.get(i);
            if (OTelDocumentBuilder.isIgnoredAttribute(kv.getKey()) == false) {
                if (writeScalarAnyValue(batchBuilder, kv.getKey(), kv.getValue(), block) == false) {
                    return false;
                }
            }
        }
        batchBuilder.endObject();
        return true;
    }

    /**
     * Writes a scalar {@link AnyValue} field. Returns {@code false} for non-scalar types (ARRAY, KVLIST, BYTES,
     * VALUE_NOT_SET), which cannot be written as plain columns in the current ESCF row API.
     */
    private static boolean writeScalarAnyValue(OtlpMetricsBatchBuilder batchBuilder, String fieldName, AnyValue value, boolean block) {
        switch (value.getValueCase()) {
            case STRING_VALUE -> {
                ByteString sv = value.getStringValueBytes();
                byte[] bytes = sv.toByteArray();
                if (block) {
                    batchBuilder.blockStringField(fieldName, bytes, 0, bytes.length);
                } else {
                    batchBuilder.stringField(fieldName, bytes, 0, bytes.length);
                }
            }
            case BOOL_VALUE -> {
                if (block) {
                    batchBuilder.blockBooleanField(fieldName, value.getBoolValue());
                } else {
                    batchBuilder.booleanField(fieldName, value.getBoolValue());
                }
            }
            case INT_VALUE -> {
                if (block) {
                    batchBuilder.blockLongField(fieldName, value.getIntValue());
                } else {
                    batchBuilder.longField(fieldName, value.getIntValue());
                }
            }
            case DOUBLE_VALUE -> {
                if (block) {
                    batchBuilder.blockDoubleField(fieldName, value.getDoubleValue());
                } else {
                    batchBuilder.doubleField(fieldName, value.getDoubleValue());
                }
            }
            case ARRAY_VALUE, KVLIST_VALUE, BYTES_VALUE, VALUE_NOT_SET -> {
                return false;
            }
            default -> throw new IllegalStateException("unexpected attribute value type [" + value.getValueCase() + "]");
        }
        return true;
    }

    private static void writeByteStringBlockFieldIfNotEmpty(OtlpMetricsBatchBuilder batchBuilder, String name, ByteString value) {
        if (value != null && value.isEmpty() == false) {
            batchBuilder.blockStringField(name, value.toByteArray(), 0, value.size());
        }
    }

    private static void writeAsciiStringField(OtlpMetricsBatchBuilder batchBuilder, String name, String value) {
        if (value != null && value.isEmpty() == false) {
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            batchBuilder.stringField(name, bytes, 0, bytes.length);
        }
    }
}
