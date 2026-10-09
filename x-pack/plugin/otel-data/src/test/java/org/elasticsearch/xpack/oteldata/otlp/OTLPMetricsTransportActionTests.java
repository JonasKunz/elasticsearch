/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.Exemplar;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;

import com.google.protobuf.InvalidProtocolBufferException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OTLPMetricsTransportActionTests extends AbstractOTLPTransportActionTests {

    private OTLPMetricsTransportAction metricsAction;
    private ClusterSettings clusterSettings;

    @Override
    protected AbstractOTLPTransportAction createAction() {
        metricsAction = createMetricsAction(Settings.EMPTY);
        return metricsAction;
    }

    private OTLPMetricsTransportAction createMetricsAction(Settings settings) {
        return createMetricsAction(settings, ProjectMetadata.builder(ProjectId.DEFAULT).build());
    }

    private OTLPMetricsTransportAction createMetricsAction(Settings settings, ProjectMetadata projectMetadata) {
        ClusterService clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING, BatchIndexingEnabled.BATCH_INDEXING, XPackSettings.METRIC_EXEMPLARS_ENABLED)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().projectMetadata(Map.of(ProjectId.DEFAULT, projectMetadata)).build())
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        return new OTLPMetricsTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            client,
            clusterService,
            BigArrays.NON_RECYCLING_INSTANCE,
            settings
        );
    }

    @Override
    protected OTLPActionRequest createRequestWithData() {
        return createMetricsRequest(createMetric());
    }

    @Override
    protected OTLPActionRequest createEmptyRequest() {
        return createMetricsRequest();
    }

    @Override
    protected boolean parseHasPartialSuccess(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).hasPartialSuccess();
    }

    @Override
    protected long parseRejectedCount(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getRejectedDataPoints();
    }

    @Override
    protected String parseErrorMessage(byte[] responseBytes) throws InvalidProtocolBufferException {
        return ExportMetricsServiceResponse.parseFrom(responseBytes).getPartialSuccess().getErrorMessage();
    }

    @Override
    protected String dataStreamType() {
        return "metrics";
    }

    // --- metrics-specific tests ---

    public void testMappingHintsSettingsUpdate() throws Exception {
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM));
        assertThat(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.isDynamic(), equalTo(true));

        clusterSettings.applySettings(Settings.builder().put(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.getKey(), "histogram").build());
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_TDIGEST));

        clusterSettings.applySettings(
            Settings.builder().put(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING.getKey(), "exponential_histogram").build()
        );
        assertThat(metricsAction.defaultMappingHints, equalTo(MappingHints.DEFAULT_EXPONENTIAL_HISTOGRAM));
    }

    public void testExemplarIngestionFollowsClusterSetting() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "ms",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        metricsAction.prepareBulkRequest(createMetricsRequest(metric), bulkRequestBuilder);

        var requests = bulkRequestBuilder.request().requests();
        assertThat(requests, hasSize(2));
        IndexRequest exemplarRequest = (IndexRequest) requests.get(1);
        assertThat(exemplarRequest.index(), equalTo("exemplars-generic.otel-default"));
        assertThat(exemplarRequest.getDynamicTemplates(), equalTo(Map.of()));
        assertThat(exemplarRequest.getDynamicTemplateParams(), equalTo(Map.of()));

        clusterSettings.applySettings(Settings.builder().put(XPackSettings.METRIC_EXEMPLARS_ENABLED.getKey(), false).build());
        bulkRequestBuilder = new BulkRequestBuilder(client);
        metricsAction.prepareBulkRequest(createMetricsRequest(metric), bulkRequestBuilder);
        assertThat(bulkRequestBuilder.request().requests(), hasSize(1));
    }

    public void testExemplarDocumentsFollowMetricDocuments() throws Exception {
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "ms",
            List.of(
                OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(OtlpUtils.createLongExemplar(1_000_000L, 42L))),
                OtlpUtils.createDoubleDataPoint(4_000_000L, 0, List.of(), List.of(OtlpUtils.createLongExemplar(3_000_000L, 43L)))
            )
        );
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        AbstractOTLPTransportAction.ProcessingContext context = metricsAction.prepareBulkRequest(
            createMetricsRequest(metric),
            bulkRequestBuilder
        );

        var requests = bulkRequestBuilder.request().requests();
        assertThat(requests, hasSize(4));
        assertThat(requests.get(0).index(), equalTo("metrics-generic.otel-default"));
        assertThat(requests.get(1).index(), equalTo("metrics-generic.otel-default"));
        assertThat(requests.get(2).index(), equalTo("exemplars-generic.otel-default"));
        assertThat(requests.get(3).index(), equalTo("exemplars-generic.otel-default"));
        assertTrue(context.isPrimaryTelemetryDoc(0));
        assertTrue(context.isPrimaryTelemetryDoc(1));
        assertFalse(context.isPrimaryTelemetryDoc(2));
        assertFalse(context.isPrimaryTelemetryDoc(3));
    }

    public void testExemplarWithoutTargetProducesWarning() throws Exception {
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(
                OtlpUtils.createDoubleDataPoint(
                    2_000_000L,
                    0,
                    List.of(keyValue("elasticsearch.index", "custom-index")),
                    List.of(OtlpUtils.createLongExemplar(1_000_000L, 42L))
                )
            )
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(new BulkItemResponse[] { successResponse() }, 0)
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(
            parseErrorMessage(responseBytes),
            equalTo("1 exemplars were dropped because no exemplar data stream can be derived from an explicit index target.\n")
        );
    }

    public void testExemplarWithoutValueProducesWarning() throws Exception {
        Exemplar exemplar = Exemplar.newBuilder().setTimeUnixNano(1_000_000L).build();
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(new BulkItemResponse[] { successResponse() }, 0)
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(parseErrorMessage(responseBytes), equalTo("1 exemplars were dropped because they have no value.\n"));
    }

    public void testDuplicateExemplarWarning() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar, exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(new BulkItemResponse[] { successResponse(), successResponse() }, 0)
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(parseErrorMessage(responseBytes), equalTo("1 exemplars were dropped due to duplicate timestamps and series identity"));
    }

    public void testExemplarFailureStoreRedirectAndDuplicateWarnings() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar, exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(new BulkItemResponse[] { successResponse(), failureStoreUsedResponse() }, 0)
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(
            parseErrorMessage(responseBytes),
            equalTo(
                "Redirected 1 exemplar documents to the failure store.\n"
                    + "1 exemplars were dropped due to duplicate timestamps and series identity"
            )
        );
    }

    public void testExemplarIndexingFailureDoesNotRejectDataPoint() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(
                new BulkItemResponse[] {
                    successResponse(),
                    bulkItemFailure("exemplars-generic.otel-default", RestStatus.FORBIDDEN, "unauthorized") },
                0
            )
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(
            parseErrorMessage(responseBytes),
            equalTo("Failed to index 1 exemplar documents. Sample error message: java.lang.RuntimeException: unauthorized\n")
        );
    }

    /**
     * An already indexed exemplar must not hide rejected data points when multiple metrics share a failed document.
     */
    public void testExemplarConflictDoesNotSuppressAllDataPointsRejected() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric firstMetric = OtlpUtils.createGaugeMetric(
            "first.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        Metric secondMetric = OtlpUtils.createGaugeMetric(
            "second.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of()))
        );
        OTLPActionRequest request = createMetricsRequest(firstMetric, secondMetric);
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
        AbstractOTLPTransportAction.ProcessingContext context = createAction().prepareBulkRequest(request, bulkRequestBuilder);
        assertThat(context.totalItems(), equalTo(2));
        assertThat(bulkRequestBuilder.numberOfActions(), equalTo(2));
        assertTrue(context.isPrimaryTelemetryDoc(0));
        assertFalse(context.isPrimaryTelemetryDoc(1));

        OTLPActionResponse response = executeRequest(
            request,
            new BulkResponse(
                new BulkItemResponse[] {
                    bulkItemFailure("metrics-generic.otel-default", RestStatus.BAD_REQUEST, "bad request"),
                    bulkItemFailure("exemplars-generic.otel-default", RestStatus.CONFLICT, "version conflict") },
                0
            )
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(2L));
        assertThat(parseErrorMessage(responseBytes), containsString("returned status [BAD_REQUEST] for 1 documents"));
        assertThat(
            parseErrorMessage(responseBytes),
            containsString("Skipped 1 duplicate documents that were already indexed with the same id.\n")
        );
    }

    public void testExemplarConflictIsReportedAsDuplicate() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metric = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );

        OTLPActionResponse response = executeRequest(
            createMetricsRequest(metric),
            new BulkResponse(
                new BulkItemResponse[] {
                    bulkItemFailure("metrics-generic.otel-default", RestStatus.CONFLICT, "version conflict"),
                    bulkItemFailure("exemplars-generic.otel-default", RestStatus.CONFLICT, "version conflict") },
                0
            )
        );

        byte[] responseBytes = response.getResponse().array();
        assertThat(parseRejectedCount(responseBytes), equalTo(0L));
        assertThat(
            parseErrorMessage(responseBytes),
            equalTo("Skipped 2 duplicate documents that were already indexed with the same id.\n")
        );
    }

    public void testSameTimestampExemplarsForDifferentMetricsAreNotDuplicates() throws Exception {
        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric firstMetric = OtlpUtils.createGaugeMetric(
            "first.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        Metric secondMetric = OtlpUtils.createGaugeMetric(
            "second.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);

        metricsAction.prepareBulkRequest(createMetricsRequest(firstMetric, secondMetric), bulkRequestBuilder);

        var requests = bulkRequestBuilder.request().requests();
        assertThat(requests, hasSize(3));
        IndexRequest firstExemplarRequest = (IndexRequest) requests.get(1);
        IndexRequest secondExemplarRequest = (IndexRequest) requests.get(2);
        assertNotEquals(firstExemplarRequest.tsid(), secondExemplarRequest.tsid());
    }

    public void testBatchIndexingGateForExemplars() throws Exception {
        enableBatchIndexing();
        ProjectMetadata projectMetadata = escfProjectMetadata(DEFAULT_TARGET);

        ExportMetricsServiceRequest plain = ExportMetricsServiceRequest.parseFrom(
            createMetricsRequest(createMetric()).getRequest().streamInput()
        );
        assertEscfAttempt(plain, projectMetadata, true, 1);

        Exemplar exemplar = OtlpUtils.createLongExemplar(1_000_000L, 42L);
        Metric metricWithExemplar = OtlpUtils.createGaugeMetric(
            "test.metric",
            "",
            List.of(OtlpUtils.createDoubleDataPoint(2_000_000L, 0, List.of(), List.of(exemplar)))
        );
        ExportMetricsServiceRequest withExemplars = ExportMetricsServiceRequest.parseFrom(
            createMetricsRequest(metricWithExemplar).getRequest().streamInput()
        );

        // The exemplar is only discovered while writing the batch, and only matters when exemplar ingestion is enabled.
        assertEscfAttempt(withExemplars, projectMetadata, true, FALLBACK);
        assertEscfAttempt(withExemplars, projectMetadata, false, 1);
    }

    public void testAttributeFanoutReturns413() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_CONTENT_LENGTH.getKey(), "1kb")
            .put(HttpTransportSettings.SETTING_HTTP_MAX_PROTOBUF_EXPANDED_CONTENT_LENGTH.getKey(), "10kb")
            .build();
        OTLPMetricsTransportAction action = createMetricsAction(settings);

        // Distinct data-point attributes force separate documents; a large resource attribute is copied into each.
        String largeValue = "x".repeat(1024);
        List<Metric> metrics = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            metrics.add(
                OtlpUtils.createGaugeMetric(
                    "test.metric",
                    "",
                    List.of(OtlpUtils.createDoubleDataPoint(i, i, List.of(keyValue("series", String.valueOf(i)))))
                )
            );
        }
        OTLPActionRequest request = new OTLPActionRequest(
            new BytesArray(
                OtlpUtils.createMetricsRequest(
                    List.of(keyValue("resource.large", largeValue), keyValue("service.name", "test-service")),
                    metrics
                ).toByteArray()
            )
        );

        @SuppressWarnings("unchecked")
        ActionListener<OTLPActionResponse> responseListener = mock(ActionListener.class);
        action.doExecute(null, request, responseListener);

        ArgumentCaptor<Exception> exception = ArgumentCaptor.forClass(Exception.class);
        verify(responseListener).onFailure(exception.capture());
        assertThat(ExceptionsHelper.status(exception.getValue()), equalTo(RestStatus.REQUEST_ENTITY_TOO_LARGE));
        assertThat(exception.getValue().getMessage(), containsString("expanded content would exceed limit"));
        verify(client, never()).execute(any(), any(), any());
    }

    /**
     * A data point whose resource carries an ARRAY attribute must make the optimistic ESCF attempt back out
     * so the transport action falls back to doc-mode, leaving nothing behind in the bulk request.
     */
    public void testEscfAttemptBacksOutForArrayResourceAttribute() throws Exception {
        enableBatchIndexing();
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("tags", "a", "b")), // ARRAY attribute
                    List.of(
                        OtlpUtils.createScopeMetrics(
                            "s",
                            "1",
                            List.of(OtlpUtils.createGaugeMetric("cpu", "1", List.of(OtlpUtils.createDoubleDataPoint(0))))
                        )
                    )
                )
            )
            .build();

        assertEscfAttempt(request, escfProjectMetadata(DEFAULT_TARGET), false, FALLBACK);
    }

    /**
     * A data point whose attribute list contains a duplicate key is rejected by {@code EscfBatchBuilder} with an
     * {@code IllegalArgumentException}; the ESCF attempt must turn that into a clean fallback instead of a 500.
     */
    public void testEscfAttemptBacksOutForDuplicateDataPointAttributeKey() throws Exception {
        enableBatchIndexing();
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        OtlpUtils.createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                OtlpUtils.createGaugeMetric(
                                    "cpu",
                                    "1",
                                    List.of(
                                        OtlpUtils.createDoubleDataPoint(
                                            0,
                                            0,
                                            List.of(keyValue("env", "prod"), keyValue("env", "staging")) // duplicate key
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
            .build();

        assertEscfAttempt(request, escfProjectMetadata(DEFAULT_TARGET), false, FALLBACK);
    }

    /**
     * When only a later data point is unsupported, the rows already written for the earlier ones must be rolled
     * back as well: the bulk request ends up with no index requests and no pre-built batch.
     */
    public void testEscfAttemptBacksOutAfterPartialProgress() throws Exception {
        enableBatchIndexing();
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        OtlpUtils.createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                OtlpUtils.createGaugeMetric("cpu", "1", List.of(OtlpUtils.createDoubleDataPoint(0))),
                                OtlpUtils.createGaugeMetric("mem", "1", List.of(OtlpUtils.createDoubleDataPoint(1_000_000L))),
                                OtlpUtils.createHistogramMetric(
                                    "latency",
                                    "ms",
                                    List.of(HistogramDataPoint.newBuilder().setTimeUnixNano(2_000_000L).setCount(1).setSum(1.0).build()),
                                    AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
                                )
                            )
                        )
                    )
                )
            )
            .build();

        assertEscfAttempt(request, escfProjectMetadata(DEFAULT_TARGET), false, FALLBACK);
    }

    /** Scalar-only data points with unique attribute keys go through the ESCF path. */
    public void testEscfAttemptSucceedsForScalarAttributes() throws Exception {
        enableBatchIndexing();
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("host.name", "h1")),
                    List.of(
                        OtlpUtils.createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                OtlpUtils.createGaugeMetric(
                                    "cpu",
                                    "1",
                                    List.of(OtlpUtils.createDoubleDataPoint(0, 0, List.of(keyValue("env", "prod"))))
                                )
                            )
                        )
                    )
                )
            )
            .build();

        assertEscfAttempt(request, escfProjectMetadata(DEFAULT_TARGET), false, 1);
    }

    /**
     * Data points that the document path would merge into a single document (same resource, scope, attributes and
     * timestamp) are written as one row each on the ESCF path, and each row gets its own index request.
     */
    public void testEscfAttemptWritesOneRowPerDataPoint() throws Exception {
        enableBatchIndexing();
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "svc")),
                    List.of(
                        OtlpUtils.createScopeMetrics(
                            "s",
                            "1",
                            List.of(
                                OtlpUtils.createGaugeMetric("cpu", "1", List.of(OtlpUtils.createDoubleDataPoint(1_000_000L))),
                                OtlpUtils.createGaugeMetric("mem", "1", List.of(OtlpUtils.createDoubleDataPoint(1_000_000L))),
                                OtlpUtils.createGaugeMetric(
                                    "disk",
                                    "1",
                                    List.of(OtlpUtils.createDoubleDataPoint(1_000_000L), OtlpUtils.createDoubleDataPoint(2_000_000L))
                                )
                            )
                        )
                    )
                )
            )
            .build();

        assertEscfAttempt(request, escfProjectMetadata(DEFAULT_TARGET), false, 4);
    }

    /** A request spanning several data streams cannot be a single batch and must fall back to doc-mode. */
    public void testEscfAttemptBacksOutForMultipleTargets() throws Exception {
        enableBatchIndexing();
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("data_stream.dataset", "one")),
                    List.of(
                        OtlpUtils.createScopeMetrics(
                            "s",
                            "1",
                            List.of(OtlpUtils.createGaugeMetric("cpu", "1", List.of(OtlpUtils.createDoubleDataPoint(0))))
                        )
                    )
                )
            )
            .addResourceMetrics(
                OtlpUtils.createResourceMetrics(
                    List.of(keyValue("service.name", "svc"), keyValue("data_stream.dataset", "two")),
                    List.of(
                        OtlpUtils.createScopeMetrics(
                            "s",
                            "1",
                            List.of(OtlpUtils.createGaugeMetric("cpu", "1", List.of(OtlpUtils.createDoubleDataPoint(0))))
                        )
                    )
                )
            )
            .build();

        // The first data point's data stream is fine; the second one targets a different stream.
        assertEscfAttempt(request, escfProjectMetadata("metrics-one.otel-default"), false, FALLBACK);
    }

    /** The first export to a stream that does not exist yet must take doc-mode, which creates the data stream. */
    public void testEscfAttemptBacksOutWithoutExistingDataStream() throws Exception {
        enableBatchIndexing();
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(
            createMetricsRequest(createMetric()).getRequest().streamInput()
        );
        assertEscfAttempt(request, ProjectMetadata.builder(ProjectId.DEFAULT).build(), false, FALLBACK);
    }

    public void testEscfAttemptBacksOutWhenBatchIndexingIsDisabled() throws Exception {
        assumeTrue("requires batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), false).build());
        ExportMetricsServiceRequest request = ExportMetricsServiceRequest.parseFrom(
            createMetricsRequest(createMetric()).getRequest().streamInput()
        );
        assertEscfAttempt(request, escfProjectMetadata(DEFAULT_TARGET), false, FALLBACK);
    }

    public void testEscfAttemptBacksOutForEmptyRequest() throws Exception {
        enableBatchIndexing();
        assertEscfAttempt(ExportMetricsServiceRequest.getDefaultInstance(), escfProjectMetadata(DEFAULT_TARGET), false, FALLBACK);
    }

    private static final String DEFAULT_TARGET = "metrics-generic.otel-default";
    /** Expected outcome of {@link #assertEscfAttempt}: the attempt backs out and the request is written in doc-mode. */
    private static final int FALLBACK = -1;

    private void enableBatchIndexing() {
        assumeTrue("requires batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build());
    }

    /** Metadata of a single time-series backing index with dimensions and batch indexing enabled. */
    private IndexMetadata escfIndexMetadata(String target) {
        Index index = new Index(".ds-" + target + "-000001", randomUUID());
        return IndexMetadata.builder(index.getName())
            .settings(
                settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.MODE.getKey(), "time_series")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "1969-01-01T00:00:00Z")
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "1971-01-01T00:00:00Z")
                    .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dimension")
                    .put(IndexSettings.TIME_SERIES_BATCH_INDEXING.getKey(), true)
            )
            .build();
    }

    /** Project metadata holding an ESCF-capable time-series data stream named {@code target}. */
    private ProjectMetadata escfProjectMetadata(String target) {
        IndexMetadata indexMetadata = escfIndexMetadata(target);
        DataStream dataStream = DataStream.builder(target, List.of(indexMetadata.getIndex())).setIndexMode(IndexMode.TIME_SERIES).build();
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(indexMetadata, true)
            .dataStreams(Map.of(target, dataStream), Map.of())
            .build();
    }

    /**
     * Runs {@link OTLPMetricsTransportAction#tryAddEscfBatch} on a fresh bulk request and asserts the outcome: on
     * success ({@code expectedRows >= 0}) the bulk request holds one index request per data point plus the pre-built
     * batch for the target, and the returned context counts the data points; on {@link #FALLBACK} the attempt returns
     * {@code null} and the bulk request must be left completely untouched.
     */
    private void assertEscfAttempt(
        ExportMetricsServiceRequest request,
        ProjectMetadata projectMetadata,
        boolean exemplarIngestionEnabled,
        int expectedRows
    ) throws IOException {
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(client);
        AbstractOTLPTransportAction.ProcessingContext context = metricsAction.tryAddEscfBatch(
            bulkRequestBuilder,
            request,
            projectMetadata,
            exemplarIngestionEnabled
        );
        BulkRequest bulkRequest = bulkRequestBuilder.request();
        try {
            if (expectedRows != FALLBACK) {
                assertNotNull("expected the ESCF attempt to succeed", context);
                assertThat(context.totalItems(), equalTo(expectedRows));
                assertThat(bulkRequest.numberOfActions(), equalTo(expectedRows));
                assertTrue("pre-built batch must be registered", bulkRequest.hasPreBuiltBatches());
            } else {
                assertNull("expected the ESCF attempt to back out", context);
                assertThat("fallback must not leave index requests behind", bulkRequest.numberOfActions(), equalTo(0));
                assertFalse("fallback must not leave a pre-built batch behind", bulkRequest.hasPreBuiltBatches());
            }
        } finally {
            bulkRequest.releasePreBuiltBatches();
        }
    }

    // --- helpers ---

    private static OTLPActionRequest createMetricsRequest(Metric... metrics) {
        return new OTLPActionRequest(
            new BytesArray(
                ExportMetricsServiceRequest.newBuilder()
                    .addResourceMetrics(
                        OtlpUtils.createResourceMetrics(
                            List.of(keyValue("service.name", "test-service")),
                            List.of(OtlpUtils.createScopeMetrics("test", "1.0.0", List.of(metrics)))
                        )
                    )
                    .build()
                    .toByteArray()
            )
        );
    }

    private static Metric createMetric() {
        return OtlpUtils.createGaugeMetric("test.metric", "", List.of(OtlpUtils.createDoubleDataPoint(0)));
    }

    private static ProjectMetadata batchIndexingProject(String target) {
        Index index = new Index(".ds-" + target + "-000001", randomUUID());
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(
                settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.MODE.getKey(), "time_series")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "1969-01-01T00:00:00Z")
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "1971-01-01T00:00:00Z")
                    .putList(IndexMetadata.INDEX_DIMENSIONS.getKey(), "dimension")
                    .put(IndexSettings.TIME_SERIES_BATCH_INDEXING.getKey(), true)
            )
            .build();
        DataStream dataStream = DataStream.builder(target, List.of(index)).setIndexMode(IndexMode.TIME_SERIES).build();
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(indexMetadata, true)
            .dataStreams(Map.of(target, dataStream), Map.of())
            .build();
    }

    public void testPreBuiltBatchReleasedWhenBulkFails() {
        assumeTrue("requires batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        OTLPMetricsTransportAction action = createMetricsAction(Settings.EMPTY, batchIndexingProject("metrics-generic.otel-default"));
        clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build());
        ArgumentCaptor<BulkRequest> bulkRequest = ArgumentCaptor.forClass(BulkRequest.class);
        ArgumentCaptor<ActionListener<BulkResponse>> bulkListener = ArgumentCaptor.captor();
        doNothing().when(client).execute(any(), bulkRequest.capture(), bulkListener.capture());
        @SuppressWarnings("unchecked")
        ActionListener<OTLPActionResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);

        action.doExecute(null, createMetricsRequest(createMetric()), responseListener);
        assertTrue(bulkRequest.getValue().hasPreBuiltBatches());

        bulkListener.getValue().onFailure(new IllegalStateException("bulk rejected"));
        assertFalse(bulkRequest.getValue().hasPreBuiltBatches());
        verify(responseListener).onFailure(any());
    }

    public void testPreBuiltBatchReleasedWhenBulkExecuteThrows() {
        assumeTrue("requires batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        OTLPMetricsTransportAction action = createMetricsAction(Settings.EMPTY, batchIndexingProject("metrics-generic.otel-default"));
        clusterSettings.applySettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build());
        ArgumentCaptor<BulkRequest> bulkRequest = ArgumentCaptor.forClass(BulkRequest.class);
        doThrow(new IllegalStateException("bulk rejected")).when(client).execute(any(), bulkRequest.capture(), any());
        @SuppressWarnings("unchecked")
        ActionListener<OTLPActionResponse> responseListener = mock(ActionListener.class, CALLS_REAL_METHODS);

        action.doExecute(null, createMetricsRequest(createMetric()), responseListener);

        assertFalse(bulkRequest.getValue().hasPreBuiltBatches());
        verify(responseListener).onFailure(any());
    }
}
