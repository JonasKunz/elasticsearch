/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;

import com.google.protobuf.ByteString;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Columnar counterpart to {@link MetricDocumentBuilder}: writes OTel metric data-point groups directly
 * into an {@link EscfBatchBuilder} without going through an intermediate XContent representation.
 *
 * <p>The method {@link #buildMetricRow} is optimistic: it starts writing the row straight away and returns
 * {@code true} on success. It returns {@code false} as soon as it encounters something the columnar path cannot
 * express — a data point that is not a scalar number (histogram/summary/exponential-histogram), a data point
 * carrying exemplars while exemplar ingestion is enabled, a non-scalar attribute value (ARRAY/KVLIST/BYTES), or
 * duplicate attribute keys (which {@link EscfBatchBuilder} rejects). There is deliberately no up-front eligibility
 * scan of the group: the common case is that everything is supported, and a separate pass over all attributes
 * and data points would cost as much as writing them. Callers must treat a {@code false} return as a signal to
 * fall back to the {@link MetricDocumentBuilder} XContent path and discard the whole batch: values are written
 * straight into the column builders, so the partially written row stays in the batch as an aborted row.
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

    /**
     * @param defaultMappingHints      the mapping hints to use for data points that do not override them
     * @param exemplarIngestionEnabled whether exemplars are ingested as separate documents. Exemplar documents are
     *                                 only produced by the XContent path, so when this is {@code true} a data point
     *                                 that carries exemplars makes {@link #buildMetricRow} return {@code false}.
     */
    public MetricColumnarBuilder(MappingHints defaultMappingHints, boolean exemplarIngestionEnabled) {
        this.defaultMappingHints = defaultMappingHints;
        this.exemplarIngestionEnabled = exemplarIngestionEnabled;
    }

    /**
     * Writes a single metric data-point group into the next row of {@code builder}.
     *
     * <p>Calls {@link EscfBatchBuilder#beginRow()} before writing. On success the row is left open and the caller
     * is responsible for calling {@link EscfBatchBuilder#finishRow()} to obtain the row index. On failure this
     * method has already called {@link EscfBatchBuilder#abortRow()}; the builder stays well-formed, but the aborted
     * row still occupies a row index and holds whatever was written before the failure, so callers that fall back
     * to the XContent path must discard the whole batch.
     *
     * @param batchBuilder       the batch builder whose current row will receive the fields
     * @param dataPointGroup     the group to write
     * @param dynamicTemplates   out-parameter: per-field dynamic-template names (same semantics as in
     *                           {@link MetricDocumentBuilder#buildMetricDocument})
     * @param dynamicTemplateParams out-parameter: per-field dynamic-template parameters
     * @return {@code true} if the row was written successfully; {@code false} if the group contains
     *         something the columnar path does not support and the caller should fall back to the XContent path
     */
    public boolean buildMetricRow(
        EscfBatchBuilder batchBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {
        batchBuilder.beginRow();
        boolean written;
        try {
            written = writeRow(batchBuilder, dataPointGroup, dynamicTemplates, dynamicTemplateParams);
        } catch (IllegalArgumentException e) {
            // EscfBatchBuilder throws IllegalArgumentException on duplicate field names, which OTel attribute
            // lists are allowed to contain.
            written = false;
        }
        if (written == false) {
            batchBuilder.abortRow();
        }
        return written;
    }

    private boolean writeRow(
        EscfBatchBuilder batchBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams
    ) {
        List<DataPoint> dataPoints = dataPointGroup.dataPoints();

        batchBuilder.longField("@timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getTimestampUnixNano()));
        if (dataPointGroup.getStartTimestampUnixNano() != 0) {
            batchBuilder.longField("start_timestamp", TimeUnit.NANOSECONDS.toMillis(dataPointGroup.getStartTimestampUnixNano()));
        }

        // resource
        if (writeResource(batchBuilder, dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl()) == false) {
            return false;
        }

        // data_stream
        writeDataStream(batchBuilder, dataPointGroup.targetIndex());

        // scope
        if (writeScope(batchBuilder, dataPointGroup.scope(), dataPointGroup.scopeSchemaUrl()) == false) {
            return false;
        }

        // datapoint attributes (top-level "attributes" object)
        if (writeAttributes(batchBuilder, dataPointGroup.dataPointAttributes(), 0) == false) {
            return false;
        }

        if (Strings.hasLength(dataPointGroup.unit())) {
            writeAsciiStringField(batchBuilder, MetricDocumentBuilder.UNIT_FIELD, dataPointGroup.unit());
        }

        String temporality = MetricDocumentBuilder.temporalityToString(dataPointGroup.temporality());
        if (temporality != null) {
            writeAsciiStringField(batchBuilder, MetricDocumentBuilder.TEMPORALITY_FIELD, temporality);
        }

        String metricNamesHash = dataPointGroup.getMetricNamesHash(hasher);
        writeAsciiStringField(batchBuilder, "_metric_names_hash", metricNamesHash);

        // metrics object
        long docCount = 0;
        batchBuilder.startObject("metrics");
        for (int i = 0; i < dataPoints.size(); i++) {
            DataPoint dataPoint = dataPoints.get(i);
            if (dataPoint.supportsColumnarValue() == false) {
                return false;
            }
            if (exemplarIngestionEnabled && dataPoint.getExemplars().isEmpty() == false) {
                return false;
            }
            String metricName = dataPoint.getMetricName();
            dataPoint.writeColumnarValue(batchBuilder, metricName);

            MappingHints mappingHints = defaultMappingHints.withConfigFromAttributes(dataPoint.getAttributes());
            String dynamicTemplate = dataPoint.getDynamicTemplate(mappingHints);
            if (dynamicTemplate != null) {
                String metricFieldPath = "metrics." + metricName;
                dynamicTemplates.put(metricFieldPath, dynamicTemplate);
                String unit = dataPointGroup.unit();
                if (unit != null && unit.isEmpty() == false) {
                    dynamicTemplateParams.put(metricFieldPath, Map.of(MetricDocumentBuilder.UNIT_FIELD, unit));
                }
            }
            if (mappingHints.docCount()) {
                docCount = dataPoint.getDocCount();
            }
        }
        batchBuilder.endObject();
        if (docCount > 0) {
            batchBuilder.longField("_doc_count", docCount);
        }

        return true;
    }

    // -------------------------------------------------------------------------
    // Resource / Scope / DataStream / Attributes
    // -------------------------------------------------------------------------

    private boolean writeResource(EscfBatchBuilder batchBuilder, Resource resource, ByteString schemaUrl) {
        batchBuilder.startObject("resource");
        writeByteStringFieldIfNotEmpty(batchBuilder, "schema_url", schemaUrl);
        if (writeAttributes(batchBuilder, resource.getAttributesList(), resource.getDroppedAttributesCount()) == false) {
            return false;
        }
        batchBuilder.endObject();
        return true;
    }

    private boolean writeScope(EscfBatchBuilder batchBuilder, InstrumentationScope scope, ByteString schemaUrl) {
        batchBuilder.startObject("scope");
        writeByteStringFieldIfNotEmpty(batchBuilder, "schema_url", schemaUrl);
        writeByteStringFieldIfNotEmpty(batchBuilder, "name", scope.getNameBytes());
        writeByteStringFieldIfNotEmpty(batchBuilder, "version", scope.getVersionBytes());
        if (writeAttributes(batchBuilder, scope.getAttributesList(), scope.getDroppedAttributesCount()) == false) {
            return false;
        }
        batchBuilder.endObject();
        return true;
    }

    private void writeDataStream(EscfBatchBuilder batchBuilder, TargetIndex targetIndex) {
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
     */
    private boolean writeAttributes(EscfBatchBuilder batchBuilder, List<KeyValue> attributes, int droppedCount) {
        if (droppedCount > 0) {
            batchBuilder.longField("dropped_attributes_count", droppedCount);
        }
        batchBuilder.startObject("attributes");
        for (int i = 0; i < attributes.size(); i++) {
            KeyValue kv = attributes.get(i);
            if (OTelDocumentBuilder.isIgnoredAttribute(kv.getKey()) == false) {
                if (writeScalarAnyValue(batchBuilder, kv.getKey(), kv.getValue()) == false) {
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
    private static boolean writeScalarAnyValue(EscfBatchBuilder batchBuilder, String fieldName, AnyValue value) {
        switch (value.getValueCase()) {
            case STRING_VALUE -> {
                ByteString sv = value.getStringValueBytes();
                byte[] bytes = sv.toByteArray();
                batchBuilder.stringField(fieldName, bytes, 0, bytes.length);
            }
            case BOOL_VALUE -> batchBuilder.booleanField(fieldName, value.getBoolValue());
            case INT_VALUE -> batchBuilder.longField(fieldName, value.getIntValue());
            case DOUBLE_VALUE -> batchBuilder.doubleField(fieldName, value.getDoubleValue());
            case ARRAY_VALUE, KVLIST_VALUE, BYTES_VALUE, VALUE_NOT_SET -> {
                return false;
            }
            default -> throw new IllegalStateException("unexpected attribute value type [" + value.getValueCase() + "]");
        }
        return true;
    }

    private static void writeByteStringFieldIfNotEmpty(EscfBatchBuilder batchBuilder, String name, ByteString value) {
        if (value != null && value.isEmpty() == false) {
            batchBuilder.stringField(name, value.toByteArray(), 0, value.size());
        }
    }

    private static void writeAsciiStringField(EscfBatchBuilder batchBuilder, String name, String value) {
        if (value != null && value.isEmpty() == false) {
            byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            batchBuilder.stringField(name, bytes, 0, bytes.length);
        }
    }
}
