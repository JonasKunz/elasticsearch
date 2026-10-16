/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.hash.BufferedMurmur3Hasher;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createDoubleDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createGaugeMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createHistogramMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongDataPoint;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createLongExemplar;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createResourceMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createScopeMetrics;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.createSumMetric;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;

public class MetricColumnarBuilderTests extends ESTestCase {

    private final MetricColumnarBuilder builder = new MetricColumnarBuilder(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM, false);
    private final long nowNanos = System.currentTimeMillis() * 1_000_000L;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Writes {@code request} with {@link MetricColumnarBuilder#buildRows}, the way the transport action does, and
     * reports the outcome per data point: {@code [true, ...]} with one entry per written row, or {@code [false]} if
     * the request cannot be written columnar (the batch must be discarded then).
     */
    private List<Boolean> writeRows(
        MetricColumnarBuilder columnarBuilder,
        OtlpMetricsBatchBuilder batchBuilder,
        ExportMetricsServiceRequest request
    ) {
        MetricColumnarBuilder.Rows rows = columnarBuilder.buildRows(batchBuilder, new BytesArray(request.toByteArray()), target -> true);
        return rows == null ? List.of(false) : Collections.nCopies(rows.rowRequests().size(), true);
    }

    private List<Boolean> writeRows(OtlpMetricsBatchBuilder batchBuilder, ExportMetricsServiceRequest request) {
        return writeRows(builder, batchBuilder, request);
    }

    /** Returns the string values stored in the named column, in row order. */
    private List<String> readStringColumn(EscfBatch batch, String path) {
        SourceSchema schema = batch.schema();
        for (int i = 0; i < schema.leafCount(); i++) {
            if (path.equals(schema.getFullPath(i))) {
                assertEquals(EscfColumnKind.STRING, batch.column(i).leafValueKind());
                ObjectTupleCursor<BytesRef> cursor = batch.column(i).bytesRefCursor(false);
                List<String> values = new ArrayList<>();
                while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    values.add(cursor.value().utf8ToString());
                }
                return values;
            }
        }
        return List.of(); // column absent
    }

    /** Returns the long values stored in the named column, in row order. */
    private List<Long> readLongColumn(EscfBatch batch, String path) {
        SourceSchema schema = batch.schema();
        for (int i = 0; i < schema.leafCount(); i++) {
            if (path.equals(schema.getFullPath(i))) {
                LongTupleCursor cursor = batch.column(i).longCursor();
                List<Long> values = new ArrayList<>();
                while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    values.add(cursor.longValue());
                }
                return values;
            }
        }
        return List.of();
    }

    /** Returns the double values stored in the named column, in row order. */
    private List<Double> readDoubleColumn(EscfBatch batch, String path) {
        SourceSchema schema = batch.schema();
        for (int i = 0; i < schema.leafCount(); i++) {
            if (path.equals(schema.getFullPath(i))) {
                LongTupleCursor cursor = batch.column(i).longCursor();
                List<Double> values = new ArrayList<>();
                while (cursor.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    values.add(Double.longBitsToDouble(cursor.longValue()));
                }
                return values;
            }
        }
        return List.of();
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /**
     * A single gauge row: verifies @timestamp, the resource attribute, and the metric value
     * all land in the batch with correct content.
     */
    public void testBuildMetricRowPopulatesFields() throws Exception {
        List<KeyValue> resourceAttrs = List.of(keyValue("service.name", "my-service"));
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    resourceAttrs,
                    List.of(
                        createScopeMetrics("scope", "1.0", List.of(createGaugeMetric("cpu", "1", List.of(createLongDataPoint(nowNanos)))))
                    )
                )
            )
            .build();

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(true), writeRows(batchBuilder, request));
        EscfBatch batch = batchBuilder.build();

        assertEquals(1, batch.docCount());
        assertFalse(readLongColumn(batch, "@timestamp").isEmpty());
        assertEquals(List.of("my-service"), readStringColumn(batch, "resource.attributes.service.name"));
    }

    /**
     * Two rows with distinct string resource attributes must produce distinct column values — not
     * both ending up with the last-written string (regression guard for the shared-buffer bug).
     */
    public void testDistinctResourceAttributesProduceDistinctColumnValues() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc-alpha")),
                    List.of(createScopeMetrics("s", "1", List.of(createGaugeMetric("m", "1", List.of(createDoubleDataPoint(nowNanos))))))
                )
            )
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc-beta")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(createGaugeMetric("m", "1", List.of(createDoubleDataPoint(nowNanos + 1_000_000L))))
                        )
                    )
                )
            )
            .build();

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(true, true), writeRows(batchBuilder, request));
        EscfBatch batch = batchBuilder.build();

        assertEquals(2, batch.docCount());
        List<String> names = readStringColumn(batch, "resource.attributes.service.name");
        assertEquals(2, names.size());
        // Each row must retain its own value — not both "svc-beta".
        assertTrue("expected svc-alpha and svc-beta but got " + names, names.containsAll(List.of("svc-alpha", "svc-beta")));
    }

    /**
     * A row with multiple distinct string attributes in the same list must all be staged correctly.
     * This is the within-row variant of the shared-buffer regression.
     */
    public void testMultipleStringAttributesInSameRowAreDistinct() throws Exception {
        List<KeyValue> dpAttrs = List.of(keyValue("env", "prod"), keyValue("region", "us-east-1"), keyValue("version", "v2"));
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createSumMetric(
                                    "req",
                                    "1",
                                    List.of(
                                        NumberDataPoint.newBuilder()
                                            .setTimeUnixNano(nowNanos)
                                            .setAsInt(42L)
                                            .addAllAttributes(dpAttrs)
                                            .build()
                                    ),
                                    true,
                                    AGGREGATION_TEMPORALITY_CUMULATIVE
                                )
                            )
                        )
                    )
                )
            )
            .build();

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(true), writeRows(batchBuilder, request));
        EscfBatch batch = batchBuilder.build();

        assertEquals(1, batch.docCount());
        assertEquals(List.of("prod"), readStringColumn(batch, "attributes.env"));
        assertEquals(List.of("us-east-1"), readStringColumn(batch, "attributes.region"));
        assertEquals(List.of("v2"), readStringColumn(batch, "attributes.version"));
    }

    /** A histogram data point makes buildRows return null (falls back to XContent path). */
    public void testBuildMetricRowReturnsFalseForHistogram() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createHistogramMetric(
                                    "latency",
                                    "ms",
                                    List.of(
                                        io.opentelemetry.proto.metrics.v1.HistogramDataPoint.newBuilder()
                                            .setTimeUnixNano(nowNanos)
                                            .setCount(1)
                                            .setSum(42.0)
                                            .build()
                                    ),
                                    AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
                                )
                            )
                        )
                    )
                )
            )
            .build();

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(false), writeRows(batchBuilder, request));
        // Unsupported data points are rejected before a row is started.
        assertEquals(0, batchBuilder.docCount());
    }

    /** Dotted metric names (e.g. {@code gauge.0}) are written as a nested-path column, not a top-level key. */
    public void testDottedMetricNameWritesNestedColumn() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics("s", "1", List.of(createGaugeMetric("gauge.0", "1", List.of(createLongDataPoint(nowNanos)))))
                    )
                )
            )
            .build();

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(true), writeRows(batchBuilder, request));
        EscfBatch batch = batchBuilder.build();

        assertEquals(1, batch.docCount());
        // The dotted metric name "gauge.0" must produce a column at path "metrics.gauge.0".
        assertFalse("metrics.gauge.0 column must be present", readLongColumn(batch, "metrics.gauge.0").isEmpty());
    }

    /** An ARRAY attribute makes buildRows return null. */
    public void testBuildMetricRowReturnsFalseForArrayAttribute() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createGaugeMetric(
                                    "cpu",
                                    "1",
                                    List.of(
                                        NumberDataPoint.newBuilder()
                                            .setTimeUnixNano(nowNanos)
                                            .setAsDouble(1.0)
                                            .addAttributes(keyValue("tags", "a", "b"))
                                            .build()
                                    )
                                )
                            )
                        )
                    )
                )
            )
            .build();

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(false), writeRows(batchBuilder, request));
        // The attributes of a data point are read before its row is started, so nothing was written to the (to be discarded) batch.
        assertEquals(0, batchBuilder.docCount());
    }

    /**
     * OTel attribute lists may contain duplicate keys, which {@code OtlpMetricsBatchBuilder} rejects. Resource attributes are
     * only written once their rows are known, so the duplicate must be detected when their fields are declared.
     */
    public void testBuildMetricRowReturnsFalseForDuplicateAttributeKey() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("service.name", "other")),
                    List.of(createScopeMetrics("s", "1", List.of(createGaugeMetric("cpu", "1", List.of(createDoubleDataPoint(nowNanos))))))
                )
            )
            .build();

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(false), writeRows(batchBuilder, request));
        assertEquals(0, batchBuilder.docCount());
    }

    /**
     * A data point carrying exemplars can only be written columnar when exemplar ingestion is disabled, because
     * exemplar documents are produced by the XContent path only.
     */
    public void testBuildMetricRowRejectsExemplarsOnlyWhenExemplarIngestionEnabled() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createGaugeMetric(
                                    "cpu",
                                    "1",
                                    List.of(createDoubleDataPoint(nowNanos, 0, List.of(), List.of(createLongExemplar(nowNanos, 42L))))
                                )
                            )
                        )
                    )
                )
            )
            .build();

        OtlpMetricsBatchBuilder rejecting = new OtlpMetricsBatchBuilder();
        MetricColumnarBuilder withExemplarIngestion = new MetricColumnarBuilder(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM, true);
        assertEquals(List.of(false), writeRows(withExemplarIngestion, rejecting, request));
        assertEquals(0, rejecting.docCount());

        OtlpMetricsBatchBuilder accepting = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(true), writeRows(accepting, request));
        assertEquals(1, accepting.build().docCount());
    }

    /**
     * After a row fails part-way, the same builder must still be able to write a following row correctly: the
     * half-written row is aborted (absent-filled) and must not leak values into the next one.
     */
    public void testFailedRowDoesNotCorruptNextRow() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "bad"), keyValue("tags", "a", "b")),
                    List.of(createScopeMetrics("s", "1", List.of(createGaugeMetric("cpu", "1", List.of(createDoubleDataPoint(nowNanos))))))
                )
            )
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "good")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(createGaugeMetric("cpu", "1", List.of(createDoubleDataPoint(nowNanos + 1_000_000L))))
                        )
                    )
                )
            )
            .build();

        // The failing data point is written first. A failure ends the loop in writeRows, so write the second
        // resource's data point separately to prove the builder is still usable.
        ExportMetricsServiceRequest bad = request.toBuilder()
            .clearResourceMetrics()
            .addResourceMetrics(request.getResourceMetrics(0))
            .build();
        ExportMetricsServiceRequest good = request.toBuilder()
            .clearResourceMetrics()
            .addResourceMetrics(request.getResourceMetrics(1))
            .build();

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(false), writeRows(batchBuilder, bad));
        assertEquals(List.of(true), writeRows(batchBuilder, good));
        assertEquals("the rejected resource wrote no row", 1, batchBuilder.docCount());

        EscfBatch batch = batchBuilder.build();
        assertEquals(1, batch.docCount());
        assertEquals(List.of("good"), readStringColumn(batch, "resource.attributes.service.name"));
        assertTrue("column from the failed resource must have no values", readStringColumn(batch, "resource.attributes.tags").isEmpty());
    }

    /**
     * Data points that the document path would group into one document (same resource, scope, attributes and
     * timestamp) must each get their own row, carrying only their own metric and its own {@code _metric_names_hash}.
     */
    public void testDataPointsOfOneGroupGetOneRowEach() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createGaugeMetric("cpu", "1", List.of(createDoubleDataPoint(nowNanos))),
                                createGaugeMetric("mem", "1", List.of(createLongDataPoint(nowNanos)))
                            )
                        )
                    )
                )
            )
            .build();

        // Precondition: the document path really would have grouped both data points into one document.
        DataPointGroupingContext context = new DataPointGroupingContext(
            new BufferedByteStringAccessor(),
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        context.groupDataPoints(request);
        List<DataPointGroupingContext.DataPointGroup> groups = new ArrayList<>();
        context.consume(groups::add);
        assertEquals(1, groups.size());
        assertEquals(2, groups.getFirst().dataPoints().size());

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(true, true), writeRows(batchBuilder, request));
        EscfBatch batch = batchBuilder.build();

        assertEquals(2, batch.docCount());
        assertEquals("mem has a value in exactly one row", 1, readLongColumn(batch, "metrics.mem").size());
        assertEquals("cpu has a value in exactly one row", 1, readDoubleColumn(batch, "metrics.cpu").size());
        List<String> hashes = readStringColumn(batch, "_metric_names_hash");
        assertEquals(2, hashes.size());
        assertNotEquals("each metric is its own series", hashes.get(0), hashes.get(1));
    }

    /**
     * {@code _metric_names_hash} is a dimension, so for a data point that is the only member of its group it must be
     * identical to the value the document path computes; otherwise the same series would get a different
     * {@code _tsid} depending on the indexing path.
     */
    public void testMetricNamesHashMatchesSingleMetricGroup() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(createGaugeMetric("system.cpu.load", "1", List.of(createDoubleDataPoint(nowNanos))))
                        )
                    )
                )
            )
            .build();

        DataPointGroupingContext context = new DataPointGroupingContext(
            new BufferedByteStringAccessor(),
            MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM
        );
        context.groupDataPoints(request);
        List<String> expected = new ArrayList<>();
        context.consume(group -> expected.add(group.getMetricNamesHash(new BufferedMurmur3Hasher(0))));

        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        assertEquals(List.of(true), writeRows(batchBuilder, request));
        assertEquals(expected, readStringColumn(batchBuilder.build(), "_metric_names_hash"));
    }

    /**
     * With the experimental shared value column, every data point's value lands in {@code metrics.value} as a double, no
     * per-metric column exists, and every row requests the same dynamic template for that one field.
     */
    public void testSharedMetricValueColumn() throws Exception {
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                createGaugeMetric("cpu", "1", List.of(createDoubleDataPoint(nowNanos))),
                                createGaugeMetric("mem", "By", List.of(createLongDataPoint(nowNanos)))
                            )
                        )
                    )
                )
            )
            .build();
        long expectedMem = request.getResourceMetrics(0).getScopeMetrics(0).getMetrics(1).getGauge().getDataPoints(0).getAsInt();
        double expectedCpu = request.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0).getGauge().getDataPoints(0).getAsDouble();

        MetricColumnarBuilder shared = new MetricColumnarBuilder(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM, false, true);
        OtlpMetricsBatchBuilder batchBuilder = new OtlpMetricsBatchBuilder();
        MetricColumnarBuilder.Rows rows = shared.buildRows(batchBuilder, new BytesArray(request.toByteArray()), target -> true);
        assertNotNull(rows);
        assertEquals(2, rows.rowRequests().size());
        for (var rowRequest : rows.rowRequests()) {
            assertEquals(java.util.Map.of("metrics.value", "gauge_double"), rowRequest.getDynamicTemplates());
        }

        EscfBatch batch = batchBuilder.build();
        assertEquals(List.of(expectedCpu, (double) expectedMem), readDoubleColumn(batch, "metrics.value"));
        SourceSchema schema = batch.schema();
        for (int i = 0; i < schema.leafCount(); i++) {
            assertFalse(schema.getFullPath(i), schema.getFullPath(i).equals("metrics.cpu") || schema.getFullPath(i).equals("metrics.mem"));
        }
        assertEquals(
            "the metric is still told apart by its names hash",
            2,
            new java.util.HashSet<>(readStringColumn(batch, "_metric_names_hash")).size()
        );
    }

    /**
     * Protobuf does not prescribe the order of fields on the wire. The Java runtime writes them in ascending field number order,
     * but the Go runtime used by the OpenTelemetry Collector writes them in descending order: a metric's data points precede its
     * temporality, unit and name, a scope's metrics precede the scope itself. The streaming parser must produce the same batch
     * for both encodings.
     */
    public void testFieldOrderOnTheWireDoesNotMatter() throws Exception {
        // both scopes route to the hostmetricsreceiver data stream; a single request may only target one data stream
        String cpuScraper =
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper";
        String memoryScraper =
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/memoryscraper";
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("host.name", "h1"), keyValue("port", 42)),
                    List.of(
                        createScopeMetrics(
                            cpuScraper,
                            "1.2.3",
                            List.of(
                                createSumMetric(
                                    "system.cpu.time",
                                    "s",
                                    List.of(
                                        createDoubleDataPoint(nowNanos, nowNanos - 1000, List.of(keyValue("state", "user"))),
                                        createDoubleDataPoint(nowNanos, nowNanos - 1000, List.of(keyValue("state", "system")))
                                    ),
                                    true,
                                    AGGREGATION_TEMPORALITY_CUMULATIVE
                                    // a description (field 2) between data points and name, like the collector emits it
                                ).toBuilder().setDescription("Total seconds each logical CPU spent on each mode").build(),
                                createGaugeMetric("system.cpu.utilization", "1", List.of(createDoubleDataPoint(nowNanos + 1))).toBuilder()
                                    .setDescription("Difference in system.cpu.time since the last measurement per logical CPU")
                                    .build()
                            )
                        ),
                        createScopeMetrics(
                            memoryScraper,
                            "2",
                            List.of(createGaugeMetric("mem", "By", List.of(createLongDataPoint(nowNanos))))
                        )
                    )
                )
            )
            .addResourceMetrics(
                createResourceMetrics(
                    List.of(keyValue("service.name", "svc2")),
                    List.of(
                        createScopeMetrics(
                            memoryScraper,
                            "2",
                            List.of(createGaugeMetric("mem", "By", List.of(createLongDataPoint(nowNanos))))
                        )
                    )
                )
            )
            .build();

        byte[] ascending = request.toByteArray();
        byte[] descending = serializeFieldsDescending(request);
        assertFalse("the two encodings must really differ", java.util.Arrays.equals(ascending, descending));
        assertEquals(request, ExportMetricsServiceRequest.parseFrom(descending));

        OtlpMetricsBatchBuilder ascendingBuilder = new OtlpMetricsBatchBuilder();
        MetricColumnarBuilder.Rows ascendingRows = builder.buildRows(ascendingBuilder, new BytesArray(ascending), target -> true);
        OtlpMetricsBatchBuilder descendingBuilder = new OtlpMetricsBatchBuilder();
        MetricColumnarBuilder.Rows descendingRows = builder.buildRows(descendingBuilder, new BytesArray(descending), target -> true);
        assertNotNull(ascendingRows);
        assertNotNull(descendingRows);
        assertEquals(5, descendingRows.rowRequests().size());
        assertEquals(ascendingRows.target(), descendingRows.target());
        assertArrayEquals(ascendingRows.timestampsNanos(), descendingRows.timestampsNanos());
        for (int i = 0; i < ascendingRows.rowRequests().size(); i++) {
            assertEquals(
                ascendingRows.rowRequests().get(i).getDynamicTemplates(),
                descendingRows.rowRequests().get(i).getDynamicTemplates()
            );
        }
        try (EscfBatch ascendingBatch = ascendingBuilder.build(); EscfBatch descendingBatch = descendingBuilder.build()) {
            assertEquals(ascendingBatch.data(), descendingBatch.data());
        }
    }

    /** Serializes {@code message} with the fields of every (nested) message in descending field number order. */
    private static byte[] serializeFieldsDescending(com.google.protobuf.Message message) throws java.io.IOException {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        com.google.protobuf.CodedOutputStream out = com.google.protobuf.CodedOutputStream.newInstance(bytes);
        List<java.util.Map.Entry<com.google.protobuf.Descriptors.FieldDescriptor, Object>> fields = new ArrayList<>(
            message.getAllFields().entrySet()
        );
        Collections.reverse(fields);
        for (var entry : fields) {
            com.google.protobuf.Descriptors.FieldDescriptor field = entry.getKey();
            if (field.isRepeated()) {
                for (Object element : (List<?>) entry.getValue()) {
                    writeFieldDescending(out, field, element);
                }
            } else {
                writeFieldDescending(out, field, entry.getValue());
            }
        }
        out.flush();
        return bytes.toByteArray();
    }

    private static void writeFieldDescending(
        com.google.protobuf.CodedOutputStream out,
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value
    ) throws java.io.IOException {
        int number = field.getNumber();
        switch (field.getType()) {
            case MESSAGE -> {
                byte[] nested = serializeFieldsDescending((com.google.protobuf.Message) value);
                out.writeTag(number, com.google.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED);
                out.writeUInt32NoTag(nested.length);
                out.writeRawBytes(nested);
            }
            case STRING -> out.writeString(number, (String) value);
            case BYTES -> out.writeBytes(number, (com.google.protobuf.ByteString) value);
            case BOOL -> out.writeBool(number, (Boolean) value);
            case ENUM -> out.writeEnum(number, ((com.google.protobuf.Descriptors.EnumValueDescriptor) value).getNumber());
            case INT32 -> out.writeInt32(number, (Integer) value);
            case UINT32 -> out.writeUInt32(number, (Integer) value);
            case INT64 -> out.writeInt64(number, (Long) value);
            case UINT64 -> out.writeUInt64(number, (Long) value);
            case FIXED64 -> out.writeFixed64(number, (Long) value);
            case SFIXED64 -> out.writeSFixed64(number, (Long) value);
            case FIXED32 -> out.writeFixed32(number, (Integer) value);
            case DOUBLE -> out.writeDouble(number, (Double) value);
            default -> throw new AssertionError("not used by OTLP metrics: " + field.getType());
        }
    }
}
