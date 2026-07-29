/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Exemplar;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;

/**
 * Builds Elasticsearch documents for OpenTelemetry metric exemplars stored in the {@code ::exemplars} data stream component.
 */
public class ExemplarDocumentBuilder extends OTelDocumentBuilder {

    public static final String METRIC_NAME_FIELD = "metric.name";
    public static final String DIMENSIONS_HASH_FIELD = "_dimensions_hash";
    public static final String FILTERED_ATTRIBUTES_FIELD = "filtered_attributes";
    public static final String VALUE_FIELD = "value";
    public static final String METRIC_NAMES_HASH_FIELD = "_metric_names_hash";

    private final BufferedMurmur3Hasher hasher = new BufferedMurmur3Hasher(0);

    public ExemplarDocumentBuilder(BufferedByteStringAccessor byteStringAccessor) {
        super(byteStringAccessor);
    }

    public void buildExemplarDocument(
        XContentBuilder builder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        DataPoint dataPoint,
        Exemplar exemplar,
        IndexVersion indexVersion
    ) throws IOException {
        builder.startObject();
        addEpochMillisNanosField(builder, "@timestamp", exemplar.getTimeUnixNano());
        buildResource(dataPointGroup.resource(), dataPointGroup.resourceSchemaUrl(), builder);
        buildDataStream(builder, dataPointGroup.targetIndex());
        buildScope(builder, dataPointGroup.scope(), dataPointGroup.scopeSchemaUrl());
        buildAttributes(builder, dataPointGroup.dataPointAttributes(), 0);
        if (Strings.hasLength(dataPoint.getUnit())) {
            builder.field(MetricDocumentBuilder.UNIT_FIELD, dataPoint.getUnit());
        }
        String temporality = MetricDocumentBuilder.temporalityToString(dataPoint.getTemporality());
        if (temporality != null) {
            builder.field(MetricDocumentBuilder.TEMPORALITY_FIELD, temporality);
        }
        builder.startObject("metric");
        builder.field("name", dataPoint.getMetricName());
        builder.endObject();
        addTraceId(builder, exemplar.getTraceId().toByteArray());
        addSpanId(builder, exemplar.getSpanId().toByteArray());
        builder.field(VALUE_FIELD, exemplarValue(exemplar));
        List<KeyValue> filteredAttributes = exemplar.getFilteredAttributesList();
        if (filteredAttributes.isEmpty() == false) {
            builder.startObject(FILTERED_ATTRIBUTES_FIELD);
            for (int i = 0, size = filteredAttributes.size(); i < size; i++) {
                KeyValue attribute = filteredAttributes.get(i);
                builder.field(attribute.getKey());
                buildAnyValue(builder, attribute.getValue());
            }
            builder.endObject();
        }
        BytesRef seriesTsid = buildSeriesTsid(dataPointGroup, dataPoint, indexVersion);
        builder.field(DIMENSIONS_HASH_FIELD, TimeSeriesIdFieldMapper.encodeTsid(seriesTsid).toString());
        builder.endObject();
    }

    static BytesRef buildSeriesTsid(
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        DataPoint dataPoint,
        BufferedMurmur3Hasher hasher,
        IndexVersion indexVersion
    ) {
        TsidBuilder tsidBuilder = new TsidBuilder(dataPointGroup.tsidBuilder().size() + 1);
        tsidBuilder.addAll(dataPointGroup.tsidBuilder());
        tsidBuilder.addStringDimension(METRIC_NAMES_HASH_FIELD, hashMetricName(dataPoint.getMetricName(), hasher));
        return tsidBuilder.buildTsid(indexVersion);
    }

    private BytesRef buildSeriesTsid(
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        DataPoint dataPoint,
        IndexVersion indexVersion
    ) {
        return buildSeriesTsid(dataPointGroup, dataPoint, hasher, indexVersion);
    }

    static String hashMetricName(String metricName, BufferedMurmur3Hasher hasher) {
        hasher.reset();
        hasher.addString(metricName);
        return Integer.toHexString(hasher.digestHash().hashCode());
    }

    private static double exemplarValue(Exemplar exemplar) {
        return switch (exemplar.getValueCase()) {
            case AS_DOUBLE -> exemplar.getAsDouble();
            case AS_INT -> exemplar.getAsInt();
            case VALUE_NOT_SET -> throw new IllegalArgumentException("exemplar without a value");
        };
    }
}
