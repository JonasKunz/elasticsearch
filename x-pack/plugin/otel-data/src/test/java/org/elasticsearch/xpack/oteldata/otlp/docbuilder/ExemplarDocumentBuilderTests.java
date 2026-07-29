/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Exemplar;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.OtlpUtils;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createExemplar;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;

public class ExemplarDocumentBuilderTests extends ESTestCase {

    private static final byte[] TRACE_ID = new byte[16];
    private static final byte[] SPAN_ID = new byte[8];

    static {
        for (int i = 0; i < TRACE_ID.length; i++) {
            TRACE_ID[i] = (byte) i;
        }
        for (int i = 0; i < SPAN_ID.length; i++) {
            SPAN_ID[i] = (byte) (16 + i);
        }
    }

    private final ExemplarDocumentBuilder documentBuilder = new ExemplarDocumentBuilder(new BufferedByteStringAccessor());
    private final DataPointGroupingContext dataPointGroupingContext = new DataPointGroupingContext(
        new BufferedByteStringAccessor(),
        MappingHints.DEFAULT_TDIGEST
    );
    private final long dataPointTimestamp = 1_704_067_713_467_654_000L;
    private final long exemplarTimestamp = 1_704_067_713_467_655_123L;
    private final IndexVersion indexVersion = IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG);

    public void testBuildExemplarDocument() throws IOException {
        List<KeyValue> dataPointAttributes = List.of(keyValue("operation", "checkout"));
        Exemplar exemplar = createExemplar(exemplarTimestamp, 42.5, TRACE_ID, SPAN_ID, keyValue("http.route", "/checkout"));
        Metric gaugeMetric = createGaugeMetric(
            "request.duration",
            "s",
            List.of(createDoubleDataPoint(dataPointTimestamp, dataPointTimestamp, dataPointAttributes, List.of(exemplar)))
        );
        groupMetrics(gaugeMetric);

        dataPointGroupingContext.consume(dataPointGroup -> {
            DataPoint dataPoint = dataPointGroup.dataPoints().getFirst();
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            documentBuilder.buildExemplarDocument(builder, dataPointGroup, dataPoint, exemplar, indexVersion);

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));

            assertThat(doc.evaluate("@timestamp"), equalTo("1704067713467.655123"));
            assertThat(doc.evaluate("data_stream.type"), equalTo("metrics"));
            assertThat(doc.evaluate("data_stream.dataset"), equalTo("generic.otel"));
            assertThat(doc.evaluate("data_stream.namespace"), equalTo("default"));
            assertThat(doc.evaluate("resource.attributes.service\\.name"), equalTo("test-service"));
            assertThat(doc.evaluate("scope.name"), equalTo("test"));
            assertThat(doc.evaluate("attributes.operation"), equalTo("checkout"));
            assertThat(doc.evaluate("unit"), equalTo("s"));
            assertThat(doc.evaluate("temporality"), is(nullValue()));
            assertThat(doc.evaluate("metric.name"), equalTo("request.duration"));
            assertThat(doc.evaluate("trace_id"), equalTo("000102030405060708090a0b0c0d0e0f"));
            assertThat(doc.evaluate("span_id"), equalTo("1011121314151617"));
            assertThat(doc.evaluate("value"), equalTo(42.5));
            assertThat(doc.evaluate("filtered_attributes.http\\.route"), equalTo("/checkout"));
            assertThat(doc.evaluate("_dimensions_hash"), isA(String.class));

            BytesRef seriesTsid = ExemplarDocumentBuilder.buildSeriesTsid(
                dataPointGroup,
                dataPoint,
                new BufferedMurmur3Hasher(0),
                indexVersion
            );
            assertThat(doc.evaluate("_dimensions_hash"), equalTo(TimeSeriesIdFieldMapper.encodeTsid(seriesTsid).toString()));
        });
    }

    public void testIntExemplarValuePromotedToDouble() throws IOException {
        Exemplar exemplar = OtlpUtils.createIntExemplar(exemplarTimestamp, 7L, TRACE_ID, SPAN_ID);
        Metric gaugeMetric = createGaugeMetric(
            "request.count",
            "",
            List.of(createDoubleDataPoint(dataPointTimestamp, dataPointTimestamp, List.of(), List.of(exemplar)))
        );
        groupMetrics(gaugeMetric);

        dataPointGroupingContext.consume(dataPointGroup -> {
            DataPoint dataPoint = dataPointGroup.dataPoints().getFirst();
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            documentBuilder.buildExemplarDocument(builder, dataPointGroup, dataPoint, exemplar, indexVersion);

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
            assertThat(doc.evaluate("value"), equalTo(7.0));
        });
    }

    public void testExemplarDocumentWithTemporality() throws IOException {
        Exemplar exemplar = createExemplar(exemplarTimestamp, 1.0, TRACE_ID, SPAN_ID);
        Metric sumMetric = createSumMetric(
            "requests.total",
            "{requests}",
            List.of(createDoubleDataPoint(dataPointTimestamp, dataPointTimestamp, List.of(), List.of(exemplar))),
            true,
            AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
        );
        groupMetrics(sumMetric);

        dataPointGroupingContext.consume(dataPointGroup -> {
            DataPoint dataPoint = dataPointGroup.dataPoints().getFirst();
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            documentBuilder.buildExemplarDocument(builder, dataPointGroup, dataPoint, exemplar, indexVersion);

            ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
            assertThat(doc.evaluate("unit"), equalTo("{requests}"));
            assertThat(doc.evaluate("temporality"), equalTo("cumulative"));
        });
    }

    public void testDimensionsHashMatchesSingleMetricSeriesTsid() throws IOException {
        Exemplar exemplar = createExemplar(exemplarTimestamp, 1.0, TRACE_ID, SPAN_ID);
        Metric gaugeMetric = createGaugeMetric(
            "latency",
            "",
            List.of(createDoubleDataPoint(dataPointTimestamp, dataPointTimestamp, List.of(keyValue("env", "prod")), List.of(exemplar)))
        );
        groupMetrics(gaugeMetric);

        dataPointGroupingContext.consume(dataPointGroup -> {
            DataPoint dataPoint = dataPointGroup.dataPoints().getFirst();
            String metricNamesHash = ExemplarDocumentBuilder.hashMetricName("latency", new BufferedMurmur3Hasher(0));

            TsidBuilder expectedTsidBuilder = new TsidBuilder();
            expectedTsidBuilder.addStringDimension("resource.attributes.service.name", "test-service");
            expectedTsidBuilder.addStringDimension("scope.name", "test");
            expectedTsidBuilder.addStringDimension("attributes.env", "prod");
            expectedTsidBuilder.addStringDimension("_metric_names_hash", metricNamesHash);
            expectedTsidBuilder.addStringDimension("unit", "");

            BytesRef expectedTsid = expectedTsidBuilder.buildTsid(indexVersion);
            BytesRef actualTsid = ExemplarDocumentBuilder.buildSeriesTsid(
                dataPointGroup,
                dataPoint,
                new BufferedMurmur3Hasher(0),
                indexVersion
            );
            assertThat(actualTsid, equalTo(expectedTsid));
        });
    }

    private void groupMetrics(Metric metric) throws IOException {
        dataPointGroupingContext.groupDataPoints(
            ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(
                    ResourceMetrics.newBuilder()
                        .setResource(Resource.newBuilder().addAttributes(keyValue("service.name", "test-service")).build())
                        .addScopeMetrics(
                            ScopeMetrics.newBuilder().setScope(InstrumentationScope.newBuilder().setName("test")).addMetrics(metric)
                        )
                        .build()
                )
                .build()
        );
    }
}
