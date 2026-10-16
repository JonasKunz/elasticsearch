/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.oteldata.otlp.DocumentMetadata;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Columnar counterpart to {@link MetricDocumentBuilder}: writes OTel metric data points directly from the protobuf wire format of
 * an {@code ExportMetricsServiceRequest} into an {@link OtlpMetricsBatchBuilder}, without decoding the request into protobuf
 * message objects and without an intermediate XContent representation.
 *
 * <p>The payload is read in a single forward pass. Strings (attribute keys and values, metric names, scope and schema fields) are
 * handed to the batch builder as slices of the payload rather than copied into {@code ByteString}s or {@code String}s first;
 * only the few values that need to be compared or combined as strings (metric names, routing attributes, the scope name) are
 * materialized, once per metric or scope rather than once per data point. Resource and scope fields are shared by all data
 * points below them; since the number of those data points is not known while streaming, their values are
 * {@linkplain OtlpMetricsBatchBuilder#declareField declared} when the resource or scope is read, so that they keep their place in
 * the schema, and written as one repeated value for the whole range of rows after the last data point of the resource or scope.
 *
 * <p>Unlike {@link MetricDocumentBuilder}, which writes one document per group of data points that share resource, scope,
 * attributes and timestamp, this class writes <b>one row per data point</b>. Grouping data points rarely combines more than one
 * metric into a document, yet finding the groups costs a tsid hash and several map lookups per data point; the columnar path
 * therefore skips it. The consequence is that {@code _metric_names_hash} (a dimension) is always the hash of the single metric
 * name of the row, so for a series that the document path would have grouped with other metrics the resulting {@code _tsid}
 * differs between the two paths. For data points that are not grouped with others, both paths produce identical {@code _tsid},
 * {@code _id} and {@code _source}.
 *
 * <p>{@link #buildRows} is optimistic: it starts writing rows straight away and returns {@code null} as soon as it encounters
 * something the columnar path cannot express — a metric that is not a gauge or sum, a data point without a value, a data point
 * carrying exemplars while exemplar ingestion is enabled, a non-scalar attribute value (ARRAY/KVLIST/BYTES), duplicate attribute
 * keys (which {@link OtlpMetricsBatchBuilder} rejects), data points that target more than one data stream, or a payload that is
 * not well-formed protobuf (the fallback path then reports the error properly). There is deliberately no up-front eligibility
 * scan: the common case is that everything is supported, and a separate pass over all attributes and data points would cost as
 * much as writing them. Callers must treat a {@code null} return as a signal to fall back to the {@link MetricDocumentBuilder}
 * XContent path and discard the whole batch: values are written straight into the column builders, so a partially written row
 * stays in the batch as an aborted row.
 *
 * <p>The field layout emitted by this class is identical to that of {@link MetricDocumentBuilder}: same field names, same
 * nesting depth. The {@code OTLPMetricsEscfComparisonRestIT} integration test guards this contract by comparing {@code _id},
 * {@code _tsid}, and {@code _source} for each document across the two paths.
 */
public final class MetricColumnarBuilder {

    // Protobuf wire types
    private static final int WIRE_VARINT = 0;
    private static final int WIRE_FIXED64 = 1;
    private static final int WIRE_LEN = 2;
    private static final int WIRE_FIXED32 = 5;

    // ExportMetricsServiceRequest
    private static final int REQUEST_RESOURCE_METRICS = 1;
    // ResourceMetrics
    private static final int RM_RESOURCE = 1;
    private static final int RM_SCOPE_METRICS = 2;
    private static final int RM_SCHEMA_URL = 3;
    // Resource
    private static final int RESOURCE_ATTRIBUTES = 1;
    private static final int RESOURCE_DROPPED_ATTRIBUTES_COUNT = 2;
    // ScopeMetrics
    private static final int SM_SCOPE = 1;
    private static final int SM_METRICS = 2;
    private static final int SM_SCHEMA_URL = 3;
    // InstrumentationScope
    private static final int SCOPE_NAME = 1;
    private static final int SCOPE_VERSION = 2;
    private static final int SCOPE_ATTRIBUTES = 3;
    private static final int SCOPE_DROPPED_ATTRIBUTES_COUNT = 4;
    // Metric
    private static final int METRIC_NAME = 1;
    private static final int METRIC_UNIT = 3;
    private static final int METRIC_GAUGE = 5;
    private static final int METRIC_SUM = 7;
    private static final int METRIC_HISTOGRAM = 9;
    private static final int METRIC_EXPONENTIAL_HISTOGRAM = 10;
    private static final int METRIC_SUMMARY = 11;
    // Gauge / Sum
    private static final int DATA_POINTS = 1;
    private static final int SUM_AGGREGATION_TEMPORALITY = 2;
    private static final int SUM_IS_MONOTONIC = 3;
    private static final int TEMPORALITY_DELTA = 1;
    private static final int TEMPORALITY_CUMULATIVE = 2;
    // NumberDataPoint
    private static final int DP_START_TIME = 2;
    private static final int DP_TIME = 3;
    private static final int DP_AS_DOUBLE = 4;
    private static final int DP_EXEMPLARS = 5;
    private static final int DP_AS_INT = 6;
    private static final int DP_ATTRIBUTES = 7;
    // KeyValue
    private static final int KV_KEY = 1;
    private static final int KV_VALUE = 2;
    // AnyValue
    private static final int ANY_STRING = 1;
    private static final int ANY_BOOL = 2;
    private static final int ANY_INT = 3;
    private static final int ANY_DOUBLE = 4;
    private static final int ANY_ARRAY = 5;
    private static final int ANY_KVLIST = 6;
    private static final int ANY_BYTES = 7;
    // ArrayValue
    private static final int ARRAY_VALUES = 1;

    // Attribute value kinds as recorded in Attributes
    private static final byte VALUE_NONE = 0;
    private static final byte VALUE_STRING = 1;
    private static final byte VALUE_BOOL = 2;
    private static final byte VALUE_LONG = 3;
    private static final byte VALUE_DOUBLE = 4;
    private static final byte VALUE_UNSUPPORTED = 5;

    private static final byte[] KEY_ELASTICSEARCH_INDEX = utf8("elasticsearch.index");
    private static final byte[] KEY_DATA_STREAM_DATASET = utf8("data_stream.dataset");
    private static final byte[] KEY_DATA_STREAM_NAMESPACE = utf8("data_stream.namespace");
    private static final byte[] KEY_DATA_STREAM_TYPE = utf8("data_stream.type");
    private static final byte[] KEY_MAPPING_HINTS = utf8(MappingHints.MAPPING_HINTS);
    private static final byte[] KEY_DOCUMENT_ID = utf8(DocumentMetadata.DOCUMENT_ID_ATTRIBUTE);
    private static final byte[] KEY_INGEST_PIPELINE = utf8(DocumentMetadata.INGEST_PIPELINE_ATTRIBUTE);
    private static final byte[] KEY_ELASTIC_MAPPING_MODE = utf8("elastic.mapping.mode");
    private static final byte[] KEY_ENCODING_FORMAT = utf8("encoding.format");
    private static final byte[] HINT_DOC_COUNT = utf8(MappingHints.DOC_COUNT);

    private static final byte[] TEMPORALITY_DELTA_BYTES = utf8("delta");
    private static final byte[] TEMPORALITY_CUMULATIVE_BYTES = utf8("cumulative");
    private static final String TEMPORALITY_DELTA_STRING = "delta";
    private static final String TEMPORALITY_CUMULATIVE_STRING = "cumulative";

    /** Name of the column all metric values share when {@link #sharedMetricValueColumn} is set. */
    public static final String SHARED_VALUE_FIELD = "value";

    private final MappingHints defaultMappingHints;
    private final boolean exemplarIngestionEnabled;
    private final boolean sharedMetricValueColumn;
    private final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);
    // Data points of one metric arrive back to back, so remembering the last metric name avoids rehashing it.
    private String lastMetricName;
    private byte[] lastMetricNamesHash;

    // Scratch state, reused across requests.
    private final Reader reader = new Reader();
    private final Attributes resourceAttributes = new Attributes();
    private final Attributes scopeAttributes = new Attributes();
    private final Attributes dataPointAttributes = new Attributes();

    // Per-request state.
    private OtlpMetricsBatchBuilder batchBuilder;
    private Predicate<String> targetSupported;
    private String target;
    private int firstRow;
    private int sharedValueField;
    private List<IndexRequest> rowRequests;
    private long[] timestamps;
    private int numTimestamps;

    /**
     * @param defaultMappingHints      the mapping hints to use for data points that do not override them
     * @param exemplarIngestionEnabled whether exemplars are ingested as separate documents. Exemplar documents are
     *                                 only produced by the XContent path, so when this is {@code true} a data point
     *                                 that carries exemplars makes {@link #buildRows} return {@code null}.
     * @param sharedMetricValueColumn  experimental: write every metric value, converted to double, into the single
     *                                 {@code metrics.value} column instead of a column per metric name. The metric is
     *                                 then only identifiable through {@code _metric_names_hash}.
     */
    public MetricColumnarBuilder(MappingHints defaultMappingHints, boolean exemplarIngestionEnabled, boolean sharedMetricValueColumn) {
        this.defaultMappingHints = defaultMappingHints;
        this.exemplarIngestionEnabled = exemplarIngestionEnabled;
        this.sharedMetricValueColumn = sharedMetricValueColumn;
    }

    public MetricColumnarBuilder(MappingHints defaultMappingHints, boolean exemplarIngestionEnabled) {
        this(defaultMappingHints, exemplarIngestionEnabled, false);
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
     * Writes every data point of the serialized {@code ExportMetricsServiceRequest} {@code payload} as one row of
     * {@code batchBuilder}, in request order.
     *
     * <p>Returns {@code null} if the request cannot be written columnar (see the class documentation). In that case
     * the caller must discard {@code batchBuilder} and its contents, since a partially written row may remain.
     *
     * @param batchBuilder    the batch builder receiving the rows
     * @param payload         the OTLP request in protobuf wire format
     * @param targetSupported evaluated once, with the name of the first data point's target data stream, so that
     *                        streams that can never take the columnar path fail before any row is written
     * @return the rows' metadata, or {@code null} if the caller must fall back to the XContent path
     */
    @Nullable
    public Rows buildRows(OtlpMetricsBatchBuilder batchBuilder, BytesReference payload, Predicate<String> targetSupported) {
        this.batchBuilder = batchBuilder;
        this.targetSupported = targetSupported;
        this.target = null;
        this.firstRow = batchBuilder.docCount();
        this.sharedValueField = -1;
        this.rowRequests = new ArrayList<>();
        this.timestamps = new long[16];
        this.numTimestamps = 0;
        try {
            // A composite reference is flattened once here rather than copied value by value further down.
            BytesRef bytes = payload.toBytesRef();
            reader.reset(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
            if (readRequest(reader.limit) == false) {
                return null;
            }
            return new Rows(target, rowRequests, Arrays.copyOf(timestamps, numTimestamps));
        } catch (MalformedPayloadException e) {
            // The fallback path parses the payload with the protobuf runtime, which reports the problem properly.
            return null;
        } catch (IllegalArgumentException e) {
            // OtlpMetricsBatchBuilder rejects what the batch cannot hold, such as duplicate field names
            return null;
        } finally {
            this.batchBuilder = null;
            this.targetSupported = null;
            this.rowRequests = null;
            this.timestamps = null;
        }
    }

    // -------------------------------------------------------------------------
    // Request / resource / scope
    // -------------------------------------------------------------------------

    private boolean readRequest(int end) {
        while (reader.pos < end) {
            int tag = reader.readTag();
            if (fieldNumber(tag) == REQUEST_RESOURCE_METRICS && wireType(tag) == WIRE_LEN) {
                int length = reader.readLength(tag);
                if (readResourceMetrics(reader.pos + length) == false) {
                    return false;
                }
            } else {
                reader.skipField(tag);
            }
        }
        return true;
    }

    private boolean readResourceMetrics(int end) {
        // The resource comes first on the wire, but the schema URL follows the scope metrics, and the number of rows the
        // resource fields span is only known at the end: locate the resource fields first, skipping over the scope metrics.
        int resourceStart = -1, resourceEnd = -1, schemaUrlStart = -1, schemaUrlLength = 0;
        final int start = reader.pos;
        while (reader.pos < end) {
            int tag = reader.readTag();
            switch (fieldNumber(tag)) {
                case RM_RESOURCE -> {
                    int length = reader.readLength(tag);
                    resourceStart = reader.pos;
                    resourceEnd = reader.pos + length;
                    reader.pos = resourceEnd;
                }
                case RM_SCHEMA_URL -> {
                    schemaUrlLength = reader.readLength(tag);
                    schemaUrlStart = reader.pos;
                    reader.pos += schemaUrlLength;
                }
                default -> reader.skipField(tag);
            }
        }
        reader.checkPos(end);

        Attributes resource = resourceAttributes;
        resource.reset();
        if (resourceStart >= 0) {
            readResource(resource, resourceStart, resourceEnd);
        }
        if (resource.unsupported) {
            return false;
        }
        // Declare the resource fields now so that they take their place in the schema; they are written once the rows are known.
        int schemaUrlField = -1;
        batchBuilder.startObject("resource");
        if (schemaUrlLength > 0) {
            schemaUrlField = batchBuilder.declareField("schema_url");
        }
        boolean distinct = declareAttributes(resource);
        batchBuilder.endObject();
        if (distinct == false) {
            return false;
        }
        final int firstRow = batchBuilder.docCount();

        reader.pos = start;
        while (reader.pos < end) {
            int tag = reader.readTag();
            if (fieldNumber(tag) == RM_SCOPE_METRICS && wireType(tag) == WIRE_LEN) {
                int length = reader.readLength(tag);
                if (readScopeMetrics(reader.pos + length) == false) {
                    return false;
                }
            } else {
                reader.skipField(tag);
            }
        }

        final int rows = batchBuilder.docCount() - firstRow;
        if (rows > 0) {
            if (schemaUrlField >= 0) {
                batchBuilder.repeatedStringField(schemaUrlField, firstRow, rows, reader.bytes, schemaUrlStart, schemaUrlLength);
            }
            writeRepeatedAttributes(resource, firstRow, rows);
        }
        return true;
    }

    private void readResource(Attributes attributes, int start, int end) {
        reader.pos = start;
        while (reader.pos < end) {
            int tag = reader.readTag();
            switch (fieldNumber(tag)) {
                case RESOURCE_ATTRIBUTES -> {
                    int length = reader.readLength(tag);
                    readKeyValue(attributes, reader.pos + length);
                }
                case RESOURCE_DROPPED_ATTRIBUTES_COUNT -> attributes.droppedCount = (int) reader.readVarint(tag);
                default -> reader.skipField(tag);
            }
        }
        reader.checkPos(end);
    }

    private boolean readScopeMetrics(int end) {
        int scopeStart = -1, scopeEnd = -1, schemaUrlStart = -1, schemaUrlLength = 0;
        final int start = reader.pos;
        while (reader.pos < end) {
            int tag = reader.readTag();
            switch (fieldNumber(tag)) {
                case SM_SCOPE -> {
                    int length = reader.readLength(tag);
                    scopeStart = reader.pos;
                    scopeEnd = reader.pos + length;
                    reader.pos = scopeEnd;
                }
                case SM_SCHEMA_URL -> {
                    schemaUrlLength = reader.readLength(tag);
                    schemaUrlStart = reader.pos;
                    reader.pos += schemaUrlLength;
                }
                default -> reader.skipField(tag);
            }
        }
        reader.checkPos(end);

        Attributes scope = scopeAttributes;
        scope.reset();
        int nameStart = -1, nameLength = 0, versionStart = -1, versionLength = 0;
        if (scopeStart >= 0) {
            reader.pos = scopeStart;
            while (reader.pos < scopeEnd) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case SCOPE_NAME -> {
                        nameLength = reader.readLength(tag);
                        nameStart = reader.pos;
                        reader.pos += nameLength;
                    }
                    case SCOPE_VERSION -> {
                        versionLength = reader.readLength(tag);
                        versionStart = reader.pos;
                        reader.pos += versionLength;
                    }
                    case SCOPE_ATTRIBUTES -> {
                        int length = reader.readLength(tag);
                        readKeyValue(scope, reader.pos + length);
                    }
                    case SCOPE_DROPPED_ATTRIBUTES_COUNT -> scope.droppedCount = (int) reader.readVarint(tag);
                    default -> reader.skipField(tag);
                }
            }
            reader.checkPos(scopeEnd);
        }
        if (scope.unsupported) {
            return false;
        }
        final String scopeName = nameLength > 0 ? new String(reader.bytes, nameStart, nameLength, StandardCharsets.UTF_8) : "";
        final String scopeRoutingDataset = TargetIndex.extractScopeRoutingDataset(scopeName, scope.encodingFormat);
        // The target of every data point that does not route itself; computed once per scope rather than per data point.
        final TargetIndex scopeTarget = TargetIndex.evaluate(
            TargetIndex.TYPE_METRICS,
            first(scope.elasticsearchIndex, resourceAttributes.elasticsearchIndex),
            first(scope.dataset, resourceAttributes.dataset),
            first(scope.namespace, resourceAttributes.namespace),
            scopeRoutingDataset
        );

        int schemaUrlField = -1, nameField = -1, versionField = -1;
        batchBuilder.startObject("scope");
        if (schemaUrlLength > 0) {
            schemaUrlField = batchBuilder.declareField("schema_url");
        }
        if (nameLength > 0) {
            nameField = batchBuilder.declareField("name");
        }
        if (versionLength > 0) {
            versionField = batchBuilder.declareField("version");
        }
        boolean distinct = declareAttributes(scope);
        batchBuilder.endObject();
        if (distinct == false) {
            return false;
        }
        final int firstRow = batchBuilder.docCount();

        reader.pos = start;
        while (reader.pos < end) {
            int tag = reader.readTag();
            if (fieldNumber(tag) == SM_METRICS && wireType(tag) == WIRE_LEN) {
                int length = reader.readLength(tag);
                if (readMetric(reader.pos + length, scopeTarget, scopeRoutingDataset) == false) {
                    return false;
                }
            } else {
                reader.skipField(tag);
            }
        }

        final int rows = batchBuilder.docCount() - firstRow;
        if (rows > 0) {
            if (schemaUrlField >= 0) {
                batchBuilder.repeatedStringField(schemaUrlField, firstRow, rows, reader.bytes, schemaUrlStart, schemaUrlLength);
            }
            if (nameField >= 0) {
                batchBuilder.repeatedStringField(nameField, firstRow, rows, reader.bytes, nameStart, nameLength);
            }
            if (versionField >= 0) {
                batchBuilder.repeatedStringField(versionField, firstRow, rows, reader.bytes, versionStart, versionLength);
            }
            writeRepeatedAttributes(scope, firstRow, rows);
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // Metric / data points
    // -------------------------------------------------------------------------

    private boolean readMetric(int end, TargetIndex scopeTarget, @Nullable String scopeRoutingDataset) {
        int nameStart = -1, nameLength = 0, unitStart = -1, unitLength = 0;
        int dataField = -1, dataStart = -1, dataEnd = -1;
        while (reader.pos < end) {
            int tag = reader.readTag();
            switch (fieldNumber(tag)) {
                case METRIC_NAME -> {
                    nameLength = reader.readLength(tag);
                    nameStart = reader.pos;
                    reader.pos += nameLength;
                }
                case METRIC_UNIT -> {
                    unitLength = reader.readLength(tag);
                    unitStart = reader.pos;
                    reader.pos += unitLength;
                }
                case METRIC_GAUGE, METRIC_SUM -> {
                    int length = reader.readLength(tag);
                    dataField = fieldNumber(tag);
                    dataStart = reader.pos;
                    dataEnd = reader.pos + length;
                    reader.pos = dataEnd;
                }
                case METRIC_HISTOGRAM, METRIC_EXPONENTIAL_HISTOGRAM, METRIC_SUMMARY -> {
                    // not expressible as scalar columns
                    return false;
                }
                default -> reader.skipField(tag);
            }
        }
        reader.checkPos(end);
        if (dataField < 0) {
            // not a supported metric type at all
            return false;
        }

        // Temporality and monotonicity of a sum follow its data points on the wire; find them first, skipping the points.
        int temporality = 0;
        boolean monotonic = false;
        if (dataField == METRIC_SUM) {
            reader.pos = dataStart;
            while (reader.pos < dataEnd) {
                int tag = reader.readTag();
                switch (fieldNumber(tag)) {
                    case SUM_AGGREGATION_TEMPORALITY -> temporality = (int) reader.readVarint(tag);
                    case SUM_IS_MONOTONIC -> monotonic = reader.readVarint(tag) != 0;
                    default -> reader.skipField(tag);
                }
            }
            reader.checkPos(dataEnd);
        }

        final Metric metric = new Metric();
        metric.name = nameLength > 0 ? new String(reader.bytes, nameStart, nameLength, StandardCharsets.UTF_8) : "";
        metric.unitStart = unitStart;
        metric.unitLength = unitLength;
        metric.unit = unitLength > 0 ? new String(reader.bytes, unitStart, unitLength, StandardCharsets.UTF_8) : null;
        metric.dynamicTemplatePrefix = dataField == METRIC_SUM && monotonic ? "counter_" : "gauge_";
        metric.temporality = switch (temporality) {
            case TEMPORALITY_DELTA -> dataField == METRIC_SUM ? TEMPORALITY_DELTA_BYTES : null;
            case TEMPORALITY_CUMULATIVE -> dataField == METRIC_SUM ? TEMPORALITY_CUMULATIVE_BYTES : null;
            default -> null;
        };
        metric.namesHash = metricNamesHash(metric.name);
        if (sharedMetricValueColumn) {
            if (sharedValueField < 0) {
                batchBuilder.startObject("metrics");
                sharedValueField = batchBuilder.declareField(SHARED_VALUE_FIELD);
                batchBuilder.endObject();
            }
            metric.valueField = sharedValueField;
        } else {
            batchBuilder.startObject("metrics");
            metric.valueField = batchBuilder.declareField(metric.name);
            batchBuilder.endObject();
        }

        reader.pos = dataStart;
        while (reader.pos < dataEnd) {
            int tag = reader.readTag();
            if (fieldNumber(tag) == DATA_POINTS && wireType(tag) == WIRE_LEN) {
                int length = reader.readLength(tag);
                if (readNumberDataPoint(reader.pos + length, metric, scopeTarget, scopeRoutingDataset) == false) {
                    return false;
                }
            } else {
                reader.skipField(tag);
            }
        }
        reader.checkPos(dataEnd);
        // The data may precede other metric fields (serializers such as Go's write fields in descending order), so continue
        // after the whole metric rather than after the data.
        reader.pos = end;
        return true;
    }

    /** What all data points of one metric share. */
    private static final class Metric {
        String name;
        int unitStart;
        int unitLength;
        @Nullable
        String unit;
        String dynamicTemplatePrefix;
        @Nullable
        byte[] temporality;
        byte[] namesHash;
        int valueField;
    }

    private boolean readNumberDataPoint(int end, Metric metric, TargetIndex scopeTarget, @Nullable String scopeRoutingDataset) {
        final Attributes attributes = dataPointAttributes;
        attributes.reset();
        long startTimeNanos = 0, timeNanos = 0;
        boolean isDouble = false, hasValue = false, hasExemplars = false;
        long valueBits = 0;
        while (reader.pos < end) {
            int tag = reader.readTag();
            switch (fieldNumber(tag)) {
                case DP_ATTRIBUTES -> {
                    int length = reader.readLength(tag);
                    readKeyValue(attributes, reader.pos + length);
                }
                case DP_START_TIME -> startTimeNanos = reader.readFixed64(tag);
                case DP_TIME -> timeNanos = reader.readFixed64(tag);
                case DP_AS_DOUBLE -> {
                    valueBits = reader.readFixed64(tag);
                    isDouble = true;
                    hasValue = true;
                }
                case DP_AS_INT -> {
                    valueBits = reader.readFixed64(tag);
                    isDouble = false;
                    hasValue = true;
                }
                case DP_EXEMPLARS -> {
                    hasExemplars = true;
                    reader.skipField(tag);
                }
                default -> reader.skipField(tag);
            }
        }
        reader.checkPos(end);

        if (hasValue == false || attributes.unsupported) {
            return false;
        }
        if (exemplarIngestionEnabled && hasExemplars) {
            return false;
        }

        final TargetIndex targetIndex = attributes.hasTargetIndexAttribute()
            ? TargetIndex.evaluate(
                TargetIndex.TYPE_METRICS,
                first(attributes.elasticsearchIndex, scopeAttributes.elasticsearchIndex, resourceAttributes.elasticsearchIndex),
                first(attributes.dataset, scopeAttributes.dataset, resourceAttributes.dataset),
                first(attributes.namespace, scopeAttributes.namespace, resourceAttributes.namespace),
                scopeRoutingDataset
            )
            : scopeTarget;
        if (target == null) {
            target = targetIndex.index();
            if (targetSupported.test(target) == false) {
                return false;
            }
        } else if (target.equals(targetIndex.index()) == false) {
            // a bulk request carries at most one pre-built batch per target
            return false;
        }

        batchBuilder.beginRow();
        boolean written;
        try {
            written = writeRow(metric, attributes, targetIndex, startTimeNanos, timeNanos, isDouble, valueBits);
        } catch (IllegalArgumentException e) {
            // OtlpMetricsBatchBuilder throws IllegalArgumentException on duplicate field names, which OTel attribute
            // lists are allowed to contain.
            written = false;
        }
        if (written == false) {
            batchBuilder.abortRow();
            return false;
        }
        int rowIndex = batchBuilder.finishRow();
        assert rowIndex == firstRow + rowRequests.size() : "rows are written one per request, in order";

        if (numTimestamps == 0 || timestamps[numTimestamps - 1] != timeNanos) {
            timestamps = ArrayUtil.grow(timestamps, numTimestamps + 1);
            timestamps[numTimestamps++] = timeNanos;
        }
        var dynamicTemplates = Maps.<String, String>newHashMapWithExpectedSize(1);
        var dynamicTemplateParams = Maps.<String, Map<String, String>>newHashMapWithExpectedSize(1);
        if (sharedMetricValueColumn) {
            // one double gauge for all metrics; a per-metric unit or counter semantics cannot be expressed in its mapping
            dynamicTemplates.put("metrics." + SHARED_VALUE_FIELD, "gauge_double");
        } else {
            String metricFieldPath = "metrics." + metric.name;
            dynamicTemplates.put(metricFieldPath, metric.dynamicTemplatePrefix + (isDouble ? "double" : "long"));
            if (metric.unit != null) {
                dynamicTemplateParams.put(metricFieldPath, Map.of(OTelTsdbDocumentBuilder.UNIT_FIELD, metric.unit));
            }
        }
        rowRequests.add(
            new IndexRequest(target).opType(DocWriteRequest.OpType.CREATE)
                .setRequireDataStream(true)
                .setIncludeSourceOnError(false)
                .setDynamicTemplates(dynamicTemplates)
                .setDynamicTemplateParams(dynamicTemplateParams)
                .setTimeSeriesTimestamp(
                    DataStream.getCanonicalTimestampBound(Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(timeNanos)))
                )
        );
        return true;
    }

    private boolean writeRow(
        Metric metric,
        Attributes attributes,
        TargetIndex targetIndex,
        long startTimeNanos,
        long timeNanos,
        boolean isDouble,
        long valueBits
    ) {
        batchBuilder.longField("@timestamp", TimeUnit.NANOSECONDS.toMillis(timeNanos));
        if (startTimeNanos != 0) {
            batchBuilder.longField("start_timestamp", TimeUnit.NANOSECONDS.toMillis(startTimeNanos));
        }

        // resource and scope fields are written for the whole range of their rows once it is known

        if (targetIndex.isDataStream()) {
            batchBuilder.startObject("data_stream");
            writeAsciiStringField("type", targetIndex.type());
            writeAsciiStringField("dataset", targetIndex.dataset());
            writeAsciiStringField("namespace", targetIndex.namespace());
            batchBuilder.endObject();
        }

        // datapoint attributes (top-level "attributes" object)
        if (attributes.droppedCount > 0) {
            batchBuilder.longField("dropped_attributes_count", attributes.droppedCount);
        }
        batchBuilder.startObject("attributes");
        for (int i = 0; i < attributes.count; i++) {
            if (attributes.ignored[i]) {
                continue;
            }
            int field = batchBuilder.declareField(reader.bytes, attributes.keyStart[i], attributes.keyLength[i]);
            switch (attributes.kind[i]) {
                case VALUE_STRING -> batchBuilder.stringField(field, reader.bytes, attributes.valueStart[i], attributes.valueLength[i]);
                case VALUE_BOOL -> batchBuilder.booleanField(field, attributes.longValue[i] != 0);
                case VALUE_LONG -> batchBuilder.longField(field, attributes.longValue[i]);
                case VALUE_DOUBLE -> batchBuilder.doubleField(field, Double.longBitsToDouble(attributes.longValue[i]));
                default -> throw new IllegalStateException("unexpected attribute value kind [" + attributes.kind[i] + "]");
            }
        }
        batchBuilder.endObject();

        if (metric.unitLength > 0) {
            batchBuilder.stringField(OTelTsdbDocumentBuilder.UNIT_FIELD, reader.bytes, metric.unitStart, metric.unitLength);
        }
        if (metric.temporality != null) {
            batchBuilder.stringField(OTelTsdbDocumentBuilder.TEMPORALITY_FIELD, metric.temporality, 0, metric.temporality.length);
        }
        batchBuilder.stringField("_metric_names_hash", metric.namesHash, 0, metric.namesHash.length);

        if (isDouble) {
            batchBuilder.doubleField(metric.valueField, Double.longBitsToDouble(valueBits));
        } else if (sharedMetricValueColumn) {
            batchBuilder.doubleField(metric.valueField, (double) valueBits);
        } else {
            batchBuilder.longField(metric.valueField, valueBits);
        }

        if (defaultMappingHints.docCount() || attributes.docCountHint) {
            // a number data point always counts as one document
            batchBuilder.longField("_doc_count", 1L);
        }
        return true;
    }

    /**
     * The {@code _metric_names_hash} dimension of a document holding exactly this one metric; computed the same way
     * as {@code DataPointGroupingContext.DataPointGroup#getMetricNamesHash} does for a single-metric group.
     */
    private byte[] metricNamesHash(String metricName) {
        if (metricName.equals(lastMetricName) == false) {
            hasher.reset();
            hasher.addString(metricName);
            lastMetricNamesHash = Integer.toHexString(hasher.digestHash().hashCode()).getBytes(StandardCharsets.US_ASCII);
            lastMetricName = metricName;
        }
        return lastMetricNamesHash;
    }

    private void writeAsciiStringField(String name, String value) {
        if (value != null && value.isEmpty() == false) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            batchBuilder.stringField(name, bytes, 0, bytes.length);
        }
    }

    // -------------------------------------------------------------------------
    // Attributes
    // -------------------------------------------------------------------------

    /**
     * The attributes of one resource, scope or data point, as positions in the payload: no key or value is copied or decoded
     * unless it steers routing or mapping. Also carries the handful of per-list properties the row needs.
     */
    private static final class Attributes {
        int count;
        int[] keyStart = new int[16];
        int[] keyLength = new int[16];
        byte[] kind = new byte[16];
        int[] valueStart = new int[16];
        int[] valueLength = new int[16];
        /** The value of long and bool attributes, and the raw bits of double attributes. */
        long[] longValue = new long[16];
        /** Whether the attribute steers the indexing rather than being a field of the document. */
        boolean[] ignored = new boolean[16];
        /** The field handle of each attribute after {@link #declareAttributes}; unused for data point attributes. */
        int[] field = new int[16];
        /** The field handle of {@code dropped_attributes_count} after {@link #declareAttributes}, if the count is positive. */
        int droppedCountField;

        int droppedCount;
        /** A non-ignored attribute has a value that cannot be a scalar column; the row cannot be written columnar. */
        boolean unsupported;
        boolean docCountHint;
        // Routing attributes; materialized because they are compared and combined as strings, which is rare.
        @Nullable
        String elasticsearchIndex;
        @Nullable
        String dataset;
        @Nullable
        String namespace;
        @Nullable
        String encodingFormat;

        void reset() {
            count = 0;
            droppedCount = 0;
            unsupported = false;
            docCountHint = false;
            elasticsearchIndex = null;
            dataset = null;
            namespace = null;
            encodingFormat = null;
        }

        int add() {
            if (count == keyStart.length) {
                int capacity = count * 2;
                keyStart = Arrays.copyOf(keyStart, capacity);
                keyLength = Arrays.copyOf(keyLength, capacity);
                kind = Arrays.copyOf(kind, capacity);
                valueStart = Arrays.copyOf(valueStart, capacity);
                valueLength = Arrays.copyOf(valueLength, capacity);
                longValue = Arrays.copyOf(longValue, capacity);
                ignored = Arrays.copyOf(ignored, capacity);
                field = Arrays.copyOf(field, capacity);
            }
            return count++;
        }

        boolean hasTargetIndexAttribute() {
            return elasticsearchIndex != null || dataset != null || namespace != null;
        }
    }

    /** Reads one {@code KeyValue} message ending at {@code end} into {@code attributes}. */
    private void readKeyValue(Attributes attributes, int end) {
        final int i = attributes.add();
        attributes.keyStart[i] = reader.pos;
        attributes.keyLength[i] = 0;
        attributes.kind[i] = VALUE_NONE;
        attributes.valueStart[i] = 0;
        attributes.valueLength[i] = 0;
        attributes.longValue[i] = 0;
        int valueStart = -1, valueEnd = -1;
        while (reader.pos < end) {
            int tag = reader.readTag();
            switch (fieldNumber(tag)) {
                case KV_KEY -> {
                    attributes.keyLength[i] = reader.readLength(tag);
                    attributes.keyStart[i] = reader.pos;
                    reader.pos += attributes.keyLength[i];
                }
                case KV_VALUE -> {
                    int length = reader.readLength(tag);
                    valueStart = reader.pos;
                    valueEnd = reader.pos + length;
                    reader.pos = valueEnd;
                }
                default -> reader.skipField(tag);
            }
        }
        reader.checkPos(end);
        if (valueStart >= 0) {
            readAnyValue(attributes, i, valueStart, valueEnd);
            reader.pos = end;
        }

        final byte[] bytes = reader.bytes;
        final int keyStart = attributes.keyStart[i];
        final int keyLength = attributes.keyLength[i];
        boolean ignored = false;
        // All steering attributes start with one of these two letters; everything else is a plain field.
        if (keyLength > 0 && (bytes[keyStart] == 'e' || bytes[keyStart] == 'd')) {
            if (keyEquals(bytes, keyStart, keyLength, KEY_ELASTICSEARCH_INDEX)) {
                ignored = true;
                if (attributes.elasticsearchIndex == null) {
                    attributes.elasticsearchIndex = stringValue(attributes, i);
                }
            } else if (keyEquals(bytes, keyStart, keyLength, KEY_DATA_STREAM_DATASET)) {
                ignored = true;
                if (attributes.dataset == null) {
                    attributes.dataset = stringValue(attributes, i);
                }
            } else if (keyEquals(bytes, keyStart, keyLength, KEY_DATA_STREAM_NAMESPACE)) {
                ignored = true;
                if (attributes.namespace == null) {
                    attributes.namespace = stringValue(attributes, i);
                }
            } else if (keyEquals(bytes, keyStart, keyLength, KEY_MAPPING_HINTS)) {
                ignored = true;
                if (attributes.kind[i] == VALUE_UNSUPPORTED && valueStart >= 0) {
                    readMappingHints(attributes, valueStart, valueEnd);
                    reader.pos = end;
                }
            } else if (keyEquals(bytes, keyStart, keyLength, KEY_DATA_STREAM_TYPE)
                || keyEquals(bytes, keyStart, keyLength, KEY_DOCUMENT_ID)
                || keyEquals(bytes, keyStart, keyLength, KEY_INGEST_PIPELINE)
                || keyEquals(bytes, keyStart, keyLength, KEY_ELASTIC_MAPPING_MODE)) {
                    ignored = true;
                } else if (attributes.encodingFormat == null
                    && attributes.kind[i] == VALUE_STRING
                    && keyEquals(bytes, keyStart, keyLength, KEY_ENCODING_FORMAT)) {
                        // only meaningful for scope attributes, where it selects the dataset; harmless elsewhere
                        attributes.encodingFormat = stringValue(attributes, i);
                    }
        }
        attributes.ignored[i] = ignored;
        if (ignored == false && (attributes.kind[i] == VALUE_UNSUPPORTED || attributes.kind[i] == VALUE_NONE)) {
            attributes.unsupported = true;
        }
    }

    /** Reads the {@code AnyValue} message at {@code [start, end)} into entry {@code i} of {@code attributes}. */
    private void readAnyValue(Attributes attributes, int i, int start, int end) {
        reader.pos = start;
        while (reader.pos < end) {
            int tag = reader.readTag();
            switch (fieldNumber(tag)) {
                case ANY_STRING -> {
                    attributes.kind[i] = VALUE_STRING;
                    attributes.valueLength[i] = reader.readLength(tag);
                    attributes.valueStart[i] = reader.pos;
                    reader.pos += attributes.valueLength[i];
                }
                case ANY_BOOL -> {
                    attributes.kind[i] = VALUE_BOOL;
                    attributes.longValue[i] = reader.readVarint(tag);
                }
                case ANY_INT -> {
                    attributes.kind[i] = VALUE_LONG;
                    attributes.longValue[i] = reader.readVarint(tag);
                }
                case ANY_DOUBLE -> {
                    attributes.kind[i] = VALUE_DOUBLE;
                    attributes.longValue[i] = reader.readFixed64(tag);
                }
                case ANY_ARRAY, ANY_KVLIST, ANY_BYTES -> {
                    attributes.kind[i] = VALUE_UNSUPPORTED;
                    attributes.valueLength[i] = reader.readLength(tag);
                    attributes.valueStart[i] = reader.pos;
                    reader.pos += attributes.valueLength[i];
                }
                default -> reader.skipField(tag);
            }
        }
        reader.checkPos(end);
    }

    /**
     * Reads the {@code elasticsearch.mapping.hints} array at {@code [start, end)} the way
     * {@link MappingHints#withConfigFromAttributes} does; only the {@code _doc_count} hint affects number data points.
     */
    private void readMappingHints(Attributes attributes, int start, int end) {
        reader.pos = start;
        while (reader.pos < end) {
            int tag = reader.readTag();
            if (fieldNumber(tag) == ANY_ARRAY && wireType(tag) == WIRE_LEN) {
                int arrayEnd = reader.pos + reader.readLength(tag);
                while (reader.pos < arrayEnd) {
                    int valueTag = reader.readTag();
                    if (fieldNumber(valueTag) == ARRAY_VALUES && wireType(valueTag) == WIRE_LEN) {
                        int valueEnd = reader.pos + reader.readLength(valueTag);
                        while (reader.pos < valueEnd) {
                            int hintTag = reader.readTag();
                            if (fieldNumber(hintTag) == ANY_STRING && wireType(hintTag) == WIRE_LEN) {
                                int length = reader.readLength(tag);
                                if (keyEquals(reader.bytes, reader.pos, length, HINT_DOC_COUNT)) {
                                    attributes.docCountHint = true;
                                }
                                reader.pos += length;
                            } else {
                                reader.skipField(hintTag);
                            }
                        }
                        reader.checkPos(valueEnd);
                    } else {
                        reader.skipField(valueTag);
                    }
                }
                reader.checkPos(arrayEnd);
            } else {
                reader.skipField(tag);
            }
        }
        reader.checkPos(end);
    }

    /** The string value of attribute {@code i}, or the empty string for any other value type, as {@code AnyValue#getStringValue}. */
    private String stringValue(Attributes attributes, int i) {
        return attributes.kind[i] == VALUE_STRING
            ? new String(reader.bytes, attributes.valueStart[i], attributes.valueLength[i], StandardCharsets.UTF_8)
            : "";
    }

    /**
     * Declares the fields of {@code attributes} (and its dropped count) in the current object, remembering the handles. Returns
     * {@code false} if two attributes map to the same field: OTel attribute lists may contain duplicate keys, which the batch
     * cannot hold, and which must be detected here because the values are only written once the rows are known.
     */
    private boolean declareAttributes(Attributes attributes) {
        if (attributes.droppedCount > 0) {
            attributes.droppedCountField = batchBuilder.declareField("dropped_attributes_count");
        }
        batchBuilder.startObject("attributes");
        boolean distinct = true;
        for (int i = 0; i < attributes.count; i++) {
            if (attributes.ignored[i]) {
                continue;
            }
            int field = batchBuilder.declareField(reader.bytes, attributes.keyStart[i], attributes.keyLength[i]);
            attributes.field[i] = field;
            for (int j = 0; j < i && distinct; j++) {
                distinct = attributes.ignored[j] || attributes.field[j] != field;
            }
        }
        batchBuilder.endObject();
        return distinct;
    }

    /** Writes the declared fields of {@code attributes} into rows {@code [firstRow, firstRow + rows)}. */
    private void writeRepeatedAttributes(Attributes attributes, int firstRow, int rows) {
        if (attributes.droppedCount > 0) {
            batchBuilder.repeatedLongField(attributes.droppedCountField, firstRow, rows, attributes.droppedCount);
        }
        for (int i = 0; i < attributes.count; i++) {
            if (attributes.ignored[i]) {
                continue;
            }
            int field = attributes.field[i];
            switch (attributes.kind[i]) {
                case VALUE_STRING -> batchBuilder.repeatedStringField(
                    field,
                    firstRow,
                    rows,
                    reader.bytes,
                    attributes.valueStart[i],
                    attributes.valueLength[i]
                );
                case VALUE_BOOL -> batchBuilder.repeatedBooleanField(field, firstRow, rows, attributes.longValue[i] != 0);
                case VALUE_LONG -> batchBuilder.repeatedLongField(field, firstRow, rows, attributes.longValue[i]);
                case VALUE_DOUBLE -> batchBuilder.repeatedDoubleField(
                    field,
                    firstRow,
                    rows,
                    Double.longBitsToDouble(attributes.longValue[i])
                );
                default -> throw new IllegalStateException("unexpected attribute value kind [" + attributes.kind[i] + "]");
            }
        }
    }

    @Nullable
    private static String first(@Nullable String a, @Nullable String b) {
        return a != null ? a : b;
    }

    @Nullable
    private static String first(@Nullable String a, @Nullable String b, @Nullable String c) {
        return a != null ? a : first(b, c);
    }

    private static boolean keyEquals(byte[] bytes, int start, int length, byte[] expected) {
        return length == expected.length && Arrays.equals(bytes, start, start + length, expected, 0, length);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // -------------------------------------------------------------------------
    // Protobuf wire format
    // -------------------------------------------------------------------------

    private static int fieldNumber(int tag) {
        return tag >>> 3;
    }

    private static int wireType(int tag) {
        return tag & 7;
    }

    /** Thrown for a payload that is not well-formed protobuf; makes {@link #buildRows} fall back to the protobuf runtime. */
    private static final class MalformedPayloadException extends RuntimeException {
        MalformedPayloadException() {
            super(null, null, false, false);
        }
    }

    /**
     * A minimal protobuf wire format reader over a byte array. It decodes nothing it does not have to: length-delimited fields
     * are located, not copied, and messages are walked by their end position.
     */
    private static final class Reader {
        byte[] bytes;
        int pos;
        int limit;

        void reset(byte[] bytes, int pos, int limit) {
            this.bytes = bytes;
            this.pos = pos;
            this.limit = limit;
        }

        int readTag() {
            long tag = readRawVarint();
            if (tag <= 0 || tag > Integer.MAX_VALUE || fieldNumber((int) tag) == 0) {
                throw new MalformedPayloadException();
            }
            return (int) tag;
        }

        /** The length of the length-delimited field {@code tag}, with its end checked against the limit. */
        int readLength(int tag) {
            if (wireType(tag) != WIRE_LEN) {
                throw new MalformedPayloadException();
            }
            return readLength();
        }

        private int readLength() {
            long length = readRawVarint();
            if (length < 0 || length > limit - pos) {
                throw new MalformedPayloadException();
            }
            return (int) length;
        }

        long readVarint(int tag) {
            if (wireType(tag) != WIRE_VARINT) {
                throw new MalformedPayloadException();
            }
            return readRawVarint();
        }

        long readFixed64(int tag) {
            if (wireType(tag) != WIRE_FIXED64 || limit - pos < 8) {
                throw new MalformedPayloadException();
            }
            long value = (long) (bytes[pos] & 0xFF) | (long) (bytes[pos + 1] & 0xFF) << 8 | (long) (bytes[pos + 2] & 0xFF) << 16
                | (long) (bytes[pos + 3] & 0xFF) << 24 | (long) (bytes[pos + 4] & 0xFF) << 32 | (long) (bytes[pos + 5] & 0xFF) << 40
                | (long) (bytes[pos + 6] & 0xFF) << 48 | (long) (bytes[pos + 7] & 0xFF) << 56;
            pos += 8;
            return value;
        }

        void skipField(int tag) {
            switch (wireType(tag)) {
                case WIRE_VARINT -> readRawVarint();
                case WIRE_FIXED64 -> skip(8);
                case WIRE_LEN -> skip(readLength());
                case WIRE_FIXED32 -> skip(4);
                // groups are deprecated and never produced for OTLP
                default -> throw new MalformedPayloadException();
            }
        }

        private void skip(int n) {
            if (n > limit - pos) {
                throw new MalformedPayloadException();
            }
            pos += n;
        }

        /** A message must end exactly at its declared end. */
        void checkPos(int end) {
            if (pos != end) {
                throw new MalformedPayloadException();
            }
        }

        private long readRawVarint() {
            long result = 0;
            for (int shift = 0; shift < 64; shift += 7) {
                if (pos >= limit) {
                    throw new MalformedPayloadException();
                }
                byte b = bytes[pos++];
                result |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
            }
            throw new MalformedPayloadException();
        }
    }
}
