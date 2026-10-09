/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.Exemplar;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.oteldata.OTelPlugin;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPoint;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.TargetIndex;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.ExemplarDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MappingHints;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricColumnarBuilder;
import org.elasticsearch.xpack.oteldata.otlp.docbuilder.MetricDocumentBuilder;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Transport action for handling OpenTelemetry Protocol (OTLP) Metrics requests.
 * This action processes the incoming metrics data, groups data points, and invokes the
 * appropriate Elasticsearch bulk indexing operations to store the metrics.
 * It also handles the response according to the OpenTelemetry Protocol specifications,
 * including success, partial success responses, and errors due to bad data or server errors.
 *
 * <p>When {@link BatchIndexingEnabled} is active cluster-wide and the request targets a single data stream whose
 * backing indices support batch indexing, the metrics are optimistically written directly into an
 * {@link EscfBatch}, one row per data point, without grouping data points into documents and without a CBOR
 * intermediate representation; the coordinator derives {@code _tsid} column-major via
 * {@link org.elasticsearch.cluster.routing.ColumnarTsidCalculator}. Only metadata is checked up
 * front; whether the payload itself is expressible as scalar columns (gauges and sums with
 * string/bool/int/double attributes, no exemplars when exemplar ingestion is enabled) is discovered while
 * writing. If any data point turns out to be unsupported, the partially built batch is discarded and the whole
 * request is grouped and written through the existing XContent path instead, so a request is never split across
 * the two paths.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/otlp">OTLP Specification</a>
 */
public class OTLPMetricsTransportAction extends AbstractOTLPTransportAction {

    public static final String NAME = "indices:data/write/otlp/metrics";
    public static final ActionType<OTLPActionResponse> TYPE = new ActionType<>(NAME);

    // visible for testing
    volatile MappingHints defaultMappingHints;
    private final ClusterService clusterService;
    private final BatchIndexingEnabled batchIndexingEnabled;
    private final Recycler<BytesRef> bytesRefRecycler;
    private volatile boolean exemplarIngestionEnabled;

    @Inject
    public OTLPMetricsTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService,
        BigArrays bigArrays,
        Settings settings
    ) {
        super(NAME, transportService, actionFilters, threadPool, client, settings);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        defaultMappingHints = MappingHints.fromSettings(clusterSettings.get(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING));
        clusterSettings.addSettingsUpdateConsumer(OTelPlugin.HISTOGRAM_FIELD_TYPE_SETTING, histogramFieldTypeSetting -> {
            defaultMappingHints = MappingHints.fromSettings(histogramFieldTypeSetting);
        });
        clusterSettings.initializeAndWatch(XPackSettings.METRIC_EXEMPLARS_ENABLED, enabled -> exemplarIngestionEnabled = enabled);
        this.clusterService = clusterService;
        this.batchIndexingEnabled = new BatchIndexingEnabled(clusterSettings);
        this.bytesRefRecycler = bigArrays.bytesRefRecycler();
    }

    @Override
    protected ExportMetricsServiceResponse responseWithRejectedItems(int rejectedItems, String message) {
        ExportMetricsPartialSuccess partialSuccess = ExportMetricsPartialSuccess.newBuilder()
            .setRejectedDataPoints(rejectedItems)
            .setErrorMessage(message)
            .build();
        return ExportMetricsServiceResponse.newBuilder().setPartialSuccess(partialSuccess).build();
    }

    @Override
    protected ProcessingContext prepareBulkRequest(OTLPActionRequest request, BulkRequestBuilder bulkRequestBuilder) throws IOException {
        var metricsServiceRequest = ExportMetricsServiceRequest.parseFrom(request.getRequest().streamInput());
        ProjectMetadata projectMetadata = clusterService.state().projectState(ProjectId.DEFAULT).metadata();
        // Snapshot volatile setting to have it consistent for this request
        boolean exemplarIngestionEnabled = this.exemplarIngestionEnabled;

        // Optimistic ESCF path: it works directly on the parsed request, one row per data point, without grouping
        // data points into documents first. Only cheap metadata checks happen up front; whether the payload is
        // expressible as scalar columns is found out while writing it. On the first unsupported data point the
        // batch is dropped and the whole request is written in doc-mode below.
        ProcessingContext escfContext = tryAddEscfBatch(
            bulkRequestBuilder,
            metricsServiceRequest,
            projectMetadata,
            exemplarIngestionEnabled
        );
        if (escfContext != null) {
            return escfContext;
        }

        BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
        DataPointGroupingContext context = new DataPointGroupingContext(byteStringAccessor, defaultMappingHints);
        context.groupDataPoints(metricsServiceRequest);
        if (context.totalItems() == 0) {
            return context;
        }

        List<DataPointGroupingContext.DataPointGroup> allGroups = new ArrayList<>();
        context.consume(allGroups::add);

        long totalExpandedBytes = 0;
        MetricDocumentBuilder metricDocumentBuilder = new MetricDocumentBuilder(byteStringAccessor, defaultMappingHints);
        ExemplarDocumentBuilder exemplarDocumentBuilder = exemplarIngestionEnabled ? new ExemplarDocumentBuilder(byteStringAccessor) : null;
        for (DataPointGroupingContext.DataPointGroup group : allGroups) {
            IndexVersion indexVersion = resolveIndexVersion(projectMetadata, group);
            totalExpandedBytes = addIndexRequestDocMode(bulkRequestBuilder, metricDocumentBuilder, group, indexVersion, totalExpandedBytes);
        }
        if (exemplarDocumentBuilder != null) {
            int firstExemplarDocumentPosition = bulkRequestBuilder.numberOfActions();
            Set<ExemplarIdentity> exemplarIdentities = new HashSet<>();
            Map<String, IndexVersion> exemplarIndexVersions = new HashMap<>();
            for (DataPointGroupingContext.DataPointGroup group : allGroups) {
                totalExpandedBytes = addExemplarIndexRequests(
                    bulkRequestBuilder,
                    exemplarDocumentBuilder,
                    group,
                    context,
                    projectMetadata,
                    exemplarIndexVersions,
                    exemplarIdentities,
                    totalExpandedBytes
                );
            }
            if (bulkRequestBuilder.numberOfActions() > firstExemplarDocumentPosition) {
                context.recordFirstExemplarDocument(firstExemplarDocumentPosition);
            }
        }

        return context;
    }

    // -------------------------------------------------------------------------
    // ESCF path
    // -------------------------------------------------------------------------

    /**
     * Optimistically writes every data point of {@code request} as one row of a single {@link EscfBatch} and
     * registers the batch, together with one {@link IndexRequest} per row, on {@code bulkRequestBuilder}.
     *
     * <p>This deliberately bypasses {@link DataPointGroupingContext}: grouping data points that share resource,
     * scope, attributes and timestamp into one document rarely combines several metrics, but costs hashing and
     * map lookups for every data point. Rows are therefore written in request order, one per data point.
     *
     * <p>Returns {@code null} without modifying {@code bulkRequestBuilder} when the request cannot be written
     * this way: batch indexing is disabled, the target data stream does not exist or does not support batch
     * indexing, the data points target more than one data stream (a {@link org.elasticsearch.action.bulk.BulkRequest}
     * carries at most one pre-built batch per target), the request contains no data points, or any data point
     * cannot be expressed columnar (see {@link MetricColumnarBuilder#buildRows}). The partially built batch is
     * released and the caller must write the request through the doc-mode path instead. The index requests are
     * only added to the bulk request once every row has been written, so a request is never split across the
     * two paths. Within a single data stream, {@code BatchModeRouter} handles routing to multiple backing
     * indices automatically via timestamp-based resolution.
     *
     * @return a context counting the written data points, or {@code null} if the caller must fall back to doc-mode
     */
    // visible for testing
    @Nullable
    ProcessingContext tryAddEscfBatch(
        BulkRequestBuilder bulkRequestBuilder,
        ExportMetricsServiceRequest request,
        ProjectMetadata projectMetadata,
        boolean exemplarIngestionEnabled
    ) throws IOException {
        if (batchIndexingEnabled.isEnabled() == false) {
            return null;
        }
        MetricColumnarBuilder columnarBuilder = new MetricColumnarBuilder(defaultMappingHints, exemplarIngestionEnabled);
        try (EscfBatchBuilder batchBuilder = new EscfBatchBuilder(bytesRefRecycler)) {
            // Cheap early exit so that streams which can never take the ESCF path do not pay for writing a batch.
            MetricColumnarBuilder.Rows rows = columnarBuilder.buildRows(
                batchBuilder,
                request,
                target -> writeIndexSupportsEscf(projectMetadata, target)
            );
            if (rows == null) {
                return null;
            }
            return finishEscfBatch(bulkRequestBuilder, batchBuilder, rows, projectMetadata);
        }
    }

    /**
     * Builds the batch and hands it, together with the per-row index requests, to {@code bulkRequestBuilder}.
     * Returns {@code null} (leaving the bulk request untouched) if nothing was written or if a backing index
     * the rows resolve to does not support batch indexing.
     */
    @Nullable
    private ProcessingContext finishEscfBatch(
        BulkRequestBuilder bulkRequestBuilder,
        EscfBatchBuilder batchBuilder,
        MetricColumnarBuilder.Rows rows,
        ProjectMetadata projectMetadata
    ) throws IOException {
        List<IndexRequest> rowRequests = rows.rowRequests();
        String target = rows.target();
        if (rowRequests.isEmpty() || backingIndicesSupportEscf(projectMetadata, target, rows.timestampsNanos()) == false) {
            return null;
        }

        // All rows are kept in one flat batch; BatchModeRouter / EscfBatchScatterer handle shard scatter downstream.
        EscfBatch batch = batchBuilder.build();

        // Guard against coordinator heap exhaustion on large fan-out OTLP exports (resource/scope
        // attributes copied into every row mean the batch can be significantly larger than the
        // incoming protobuf payload).
        long batchBytes = batch.ramBytesUsed();
        if (batchBytes > maxExpandedContentLength) {
            batch.close();
            throw new ElasticsearchStatusException(
                "OTLP request rejected: ESCF batch ["
                    + batchBytes
                    + " b] would exceed expanded content limit ["
                    + maxExpandedContentLength
                    + " b]",
                RestStatus.REQUEST_ENTITY_TOO_LARGE
            );
        }

        // Attach source rows now that the batch object is stable, and only now hand the requests to the bulk
        // request: every row has been written, so there is no fallback to doc-mode past this point.
        for (int row = 0; row < rowRequests.size(); row++) {
            IndexRequest indexRequest = rowRequests.get(row);
            indexRequest.indexSource().setSourceRow(batch, row, XContentType.JSON);
            bulkRequestBuilder.add(indexRequest);
        }

        // Register the pre-built batch. BatchModeRouter will scatter it to shards and invoke
        // ForIndexDimensions.indexShard(requests, batch), which calls ColumnarTsidCalculator to
        // derive _tsid column-major (no pre-set tsid required on the IndexRequests).
        // Ownership of the batch transfers to the bulk request here. EscfBatchBuilder.close() only
        // releases unbuilt column buffers, so closing the builder does NOT close or invalidate the batch;
        // ownership passes to the bulk action, which releases it, and AbstractOTLPTransportAction releases it
        // if the bulk is rejected before the action takes it.
        bulkRequestBuilder.setPreBuiltBatches(Map.of(target, batch));
        return ProcessingContext.withTotalItems(rowRequests.size());
    }

    // -------------------------------------------------------------------------
    // Doc-mode (XContent) path — unchanged from original implementation
    // -------------------------------------------------------------------------

    private long addIndexRequestDocMode(
        BulkRequestBuilder bulkRequestBuilder,
        MetricDocumentBuilder metricDocumentBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        IndexVersion indexVersion,
        long totalExpandedBytes
    ) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
            var dynamicTemplates = Maps.<String, String>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            var dynamicTemplateParams = Maps.<String, Map<String, String>>newHashMapWithExpectedSize(dataPointGroup.dataPoints().size());
            String dataStreamName = dataPointGroup.targetIndex().index();
            BytesRef tsid = metricDocumentBuilder.buildMetricDocument(
                xContentBuilder,
                dataPointGroup,
                dynamicTemplates,
                dynamicTemplateParams,
                indexVersion
            );
            var indexRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE)
                .setRequireDataStream(true)
                .source(xContentBuilder)
                .setIncludeSourceOnError(false)
                .setDynamicTemplates(dynamicTemplates)
                .setDynamicTemplateParams(dynamicTemplateParams);
            if (indexVersion.onOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG)) {
                indexRequest.tsid(tsid);
            }
            totalExpandedBytes = accountExpandedContent(totalExpandedBytes, indexRequest);
            bulkRequestBuilder.add(indexRequest);
        }
        return totalExpandedBytes;
    }

    private IndexVersion resolveIndexVersion(ProjectMetadata projectMetadata, DataPointGroupingContext.DataPointGroup group) {
        String dataStreamName = group.targetIndex().index();
        DataStream dataStream = projectMetadata.dataStreams().get(dataStreamName);
        if (dataStream == null) {
            DataStreamAlias alias = projectMetadata.dataStreamAliases().get(dataStreamName);
            if (alias != null && alias.getWriteDataStream() != null) {
                dataStream = projectMetadata.dataStreams().get(alias.getWriteDataStream());
            }
        }
        if (dataStream == null) {
            return IndexVersion.current();
        }
        Instant ts = Instant.ofEpochMilli(group.getTimestampUnixNano() / 1_000_000L);
        Index index = dataStream.selectTimeSeriesWriteIndex(ts, projectMetadata);
        if (index == null) {
            index = dataStream.getWriteIndex();
        }
        return projectMetadata.getIndexSafe(index).getCreationVersion();
    }

    private static @Nullable DataStream resolveDataStream(ProjectMetadata projectMetadata, String dataStreamName) {
        DataStream dataStream = projectMetadata.dataStreams().get(dataStreamName);
        if (dataStream == null) {
            DataStreamAlias alias = projectMetadata.dataStreamAliases().get(dataStreamName);
            if (alias != null && alias.getWriteDataStream() != null) {
                dataStream = projectMetadata.dataStreams().get(alias.getWriteDataStream());
            }
        }
        return dataStream;
    }

    private static boolean supportsEscf(ProjectMetadata projectMetadata, Index index) {
        IndexMetadata indexMetadata = projectMetadata.getIndexSafe(index);
        return indexMetadata.getTimeSeriesDimensions().isEmpty() == false
            && IndexSettings.TIME_SERIES_BATCH_INDEXING.get(indexMetadata.getSettings());
    }

    /**
     * Early, timestamp-independent check: the data stream must already exist (the first export creates it through
     * the doc-mode path) and its current write index must support ESCF.
     */
    private static boolean writeIndexSupportsEscf(ProjectMetadata projectMetadata, String dataStreamName) {
        DataStream dataStream = resolveDataStream(projectMetadata, dataStreamName);
        if (dataStream == null || dataStream.getWriteIndex() == null) {
            return false;
        }
        return supportsEscf(projectMetadata, dataStream.getWriteIndex());
    }

    /**
     * Determines which backing indices a batch actually targets based on the timestamps of its rows, then verifies
     * that each one supports ESCF (has dimensions and batch indexing enabled).
     */
    private static boolean backingIndicesSupportEscf(ProjectMetadata projectMetadata, String dataStreamName, long[] timestampsNanos) {
        DataStream dataStream = resolveDataStream(projectMetadata, dataStreamName);
        if (dataStream == null) {
            return false;
        }
        for (Index index : dataStream.selectTimeSeriesWriteIndices(timestampsNanos, projectMetadata)) {
            if (supportsEscf(projectMetadata, index) == false) {
                return false;
            }
        }
        return true;
    }

    private long addExemplarIndexRequests(
        BulkRequestBuilder bulkRequestBuilder,
        ExemplarDocumentBuilder exemplarDocumentBuilder,
        DataPointGroupingContext.DataPointGroup dataPointGroup,
        DataPointGroupingContext context,
        ProjectMetadata projectMetadata,
        Map<String, IndexVersion> indexVersions,
        Set<ExemplarIdentity> exemplarIdentities,
        long totalExpandedBytes
    ) throws IOException {
        TargetIndex targetIndex = dataPointGroup.targetIndex().exemplarsTarget();
        if (targetIndex == null) {
            int exemplarCount = 0;
            for (DataPoint dataPoint : dataPointGroup.dataPoints()) {
                exemplarCount += dataPoint.getExemplars().size();
            }
            context.recordExemplarsWithoutTarget(exemplarCount);
            return totalExpandedBytes;
        }
        String dataStreamName = targetIndex.index();
        IndexVersion indexVersion = indexVersions.computeIfAbsent(dataStreamName, name -> resolveIndexVersion(projectMetadata, name));
        for (DataPoint dataPoint : dataPointGroup.dataPoints()) {
            if (dataPoint.getExemplars().isEmpty()) {
                continue;
            }
            BytesRef tsid = dataPointGroup.buildExemplarTsid(dataPoint.getMetricName(), indexVersion);
            for (Exemplar exemplar : dataPoint.getExemplars()) {
                if (exemplar.getValueCase() == Exemplar.ValueCase.VALUE_NOT_SET) {
                    context.recordExemplarWithoutValue();
                    continue;
                }
                ExemplarIdentity identity = new ExemplarIdentity(
                    targetIndex.index(),
                    tsid,
                    TimeUnit.NANOSECONDS.toMillis(exemplar.getTimeUnixNano())
                );
                if (exemplarIdentities.add(identity) == false) {
                    context.recordDuplicateExemplar();
                    continue;
                }
                try (XContentBuilder xContentBuilder = XContentFactory.cborBuilder(new BytesStreamOutput())) {
                    exemplarDocumentBuilder.buildExemplarDocument(xContentBuilder, dataPointGroup, dataPoint, exemplar, targetIndex);
                    IndexRequest indexRequest = new IndexRequest(targetIndex.index()).opType(DocWriteRequest.OpType.CREATE)
                        .setRequireDataStream(true)
                        .source(xContentBuilder)
                        .setIncludeSourceOnError(false);
                    if (indexVersion.onOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG)) {
                        indexRequest.tsid(tsid);
                    }
                    totalExpandedBytes = accountExpandedContent(totalExpandedBytes, indexRequest);
                    bulkRequestBuilder.add(indexRequest);
                }
            }
        }
        return totalExpandedBytes;
    }

    private static IndexVersion resolveIndexVersion(ProjectMetadata projectMetadata, String dataStreamName) {
        DataStream dataStream = projectMetadata.dataStreams().get(dataStreamName);
        if (dataStream == null) {
            DataStreamAlias alias = projectMetadata.dataStreamAliases().get(dataStreamName);
            if (alias != null && alias.getWriteDataStream() != null) {
                dataStream = projectMetadata.dataStreams().get(alias.getWriteDataStream());
            }
        }
        if (dataStream != null && dataStream.getWriteIndex() != null) {
            return projectMetadata.getIndexSafe(dataStream.getWriteIndex()).getCreationVersion();
        }
        return IndexVersion.current();
    }

    private record ExemplarIdentity(String index, BytesRef tsid, long timestamp) {}
}
