# Exemplar Store for Metrics Data Streams

## Problem Statement

OpenTelemetry exemplars are sample trace contexts (trace_id, span_id, filtered attributes)
attached to metric data points. They enable correlating a specific metric measurement back to
the distributed trace that produced it. Elasticsearch currently drops exemplars during OTLP
metrics ingestion — there is no storage, indexing, or query path for them.

## Design Alternatives

This document focuses on storing the exemplars as one document per exemplar, stored in separate indices from the metric data.
Based on this, two storage models were considered for exemplar data.

### Alternative A: Separate Linked Data Streams

Exemplars are stored in standalone data streams (e.g. `metrics-otel-default-exemplars`)
explicitly linked from the metrics data stream via `DataStreamOptions`:

```json
{ "options": { "exemplar_store": { "data_stream": "metrics-otel-default-exemplars" } } }
```

Advantages:
- Minimal core infrastructure changes — exemplar DS is just a regular data stream
- No new component selector, no write path changes
- Fully independent lifecycle, mappings, and settings

Disadvantages:
- **Does not compose with routing.** If Streams or reroute processors create per-service
  metrics data streams dynamically (e.g. `metrics-otel-checkout`, `metrics-otel-payments`),
  the exemplar routing must be separately duplicated. Each new metrics data stream needs a
  corresponding exemplar data stream created and linked independently.
- **Creation/deletion mismatch.** The exemplar data stream must be created manually and the
  link maintained. Deletion of the metrics data stream does not naturally cascade.
- **Access control duplication.** Security roles must explicitly enumerate exemplar data
  stream patterns — access does not follow from metrics permissions.
- **Template proliferation.** Every metrics index template needs a paired exemplar template.

### Alternative B: Sub-Component of the Data Stream

Exemplars are a third component of the data stream, alongside backing indices (`::data`) and
the failure store (`::failures`). Accessed via `::exemplars`.

```
metrics-otel-default::data       — TSDB backing indices
metrics-otel-default::failures   — failure store indices
metrics-otel-default::exemplars  — exemplar indices
```

Advantages:
- **Routing follows metrics.** When Streams or reroute processors create a new metrics data
  stream, exemplar storage is automatically available — the OTLP intake targets
  `<routed_target>::exemplars` and it just works.
- **Unified lifecycle management.** Creation and deletion of the exemplar component is tied
  to the data stream itself (configurable via template options, like the failure store).
- **Access control follows metrics.** A role granting access to `metrics-otel-*` implicitly
  covers `metrics-otel-*::exemplars` (same as `::data`).
- **No template proliferation.** A single composable template declares both the metrics
  settings and the exemplar component configuration.

Disadvantages:
- Requires extending `IndexComponentSelector` with a third value
- Write path must support `::exemplars` in bulk action lines
- Exemplar backing indices need their own mappings/settings, distinct from `::data` —
  requires extending how component-specific mappings are resolved from templates
- More core infrastructure changes than Alternative A
- We can't (or shouldn't) use the existing failure store infrastructure

### Conclusion

**Alternative B (sub-component)** is chosen. The routing argument is decisive: exemplar
storage must follow metrics data stream identity without manual intervention. This matches
the design philosophy of the failure store, which exists as a component precisely so that
failure handling follows the data stream through creation, deletion, routing, and access
control.

## Design

### Exemplars as a Data Stream Component

Exemplar indices are a **third component** of a data stream, enabled via template
configuration. Like the failure store, they are backing indices owned by the data stream but
with independent mappings, settings, and lifecycle.

```
.ds-metrics-otel-default-2026.07.20-000001          (::data)
.fs-metrics-otel-default-2026.07.20-000001          (::failures)
.exemplars-metrics-otel-default-2026.07.20-000001   (::exemplars)
```

### Enabling the Exemplar Store

Declared in the composable index template's data stream options (same pattern as failure
store):

```json
{
  "template": {
    "data_stream": {
      "options": {
        "exemplar_store": {
          "enabled": true
        }
      }
    }
  }
}
```

When enabled, the first exemplar backing index is created alongside the data stream (or
lazily on first write — TBD). The exemplar component inherits the data stream's identity
for routing, access control, and lifecycle scoping.

### Index Mode

Exemplar indices use **`logsdb_columnar`** index mode:
- No uniqueness constraint (multiple exemplars per series per timestamp are valid)
- Synthetic source by default (columnar storage)
- Default sort is `@timestamp DESC`; intake-specific templates may override with custom sort
  for compression (e.g. sort on a dimensions hash field + `@timestamp`)

### Component-Specific Mappings and Settings

The exemplar component's mappings and settings are defined by a referenced **index template**.
The exemplar store configuration points to this template by name:

```json
{
  "template": {
    "data_stream": {
      "options": {
        "exemplar_store": {
          "enabled": true,
          "index_template": "metrics-otel-exemplars"
        }
      }
    }
  }
}
```

The referenced index template is a standard composable index template that defines the full
mappings and settings for exemplar indices. There is no automatic inheritance from the
metrics template — the exemplar template must explicitly compose whatever component
templates it needs (e.g. shared dimension component templates).

For OTel, the exemplar index template would compose:
- Shared dimension component templates (e.g. `otel@mappings` for `resource.attributes.*`,
  `attributes.*`, etc.)
- An exemplar-specific component template defining `trace_id`, `span_id`, `value`,
  `_dimensions_hash`, and the `logsdb_columnar` index mode

### Compression (Intake-Specific Optimization)

For efficient compression, exemplars for the same series should be physically adjacent on
disk. This is an implementation detail of specific intake endpoints, not a requirement of the
exemplar store infrastructure.

Each intake endpoint achieves this through template configuration:
- **OTLP**: defines a `_dimensions_hash` field, populates it with the pre-computed `_tsid`,
  and configures `index.sort.field` and routing on it.
- **Prometheus remote write**: similarly defines a hash field computed from labels.
- **Generic clients**: may or may not optimize for compression — the exemplar store works
  regardless.

### Dimension Fields

Exemplar indices need dimension fields so that queries can correlate exemplars with metrics.
Since there is no automatic inheritance, the exemplar index template must explicitly compose
the same dimension-defining component templates as the metrics template. For OTel this means
both the metrics and exemplar templates reference shared component templates like
`otel@mappings`.

### Write Path

Clients index into the exemplar component using the `::exemplars` selector in the bulk
action line:

```json
{ "index": { "_index": "metrics-otel-default::exemplars" } }
{ "@timestamp": "...", "trace_id": "...", "span_id": "...", "value": 3.14, ... }
```

Implementation requires:
1. Adding `EXEMPLARS` to `IndexComponentSelector` enum
2. Allowing selectors in the write path (currently `IndicesOptions` for `IndexRequest`
   disables selectors via `allowSelectors(false)`)
3. Routing documents with the `::exemplars` selector to the exemplar write index (analogous
   to how `writeToFailureStore` routes to failure indices)

The OTLP and Prometheus remote write endpoints build bulk requests targeting
`<metrics_stream_name>::exemplars`.

### Read Path: ES|QL `TS_EXEMPLARS` Source Command

The primary query interface for exemplars is a dedicated ES|QL **source command**:

```esql
TS_EXEMPLARS(FROM metrics-otel-default | WHERE service.name == "checkout" | STATS AVG(request.duration))
```

`TS_EXEMPLARS` does **not** execute the inner query. Instead it inspects the query plan to
extract:
1. **Dimension filters** — predicates on dimension fields (e.g. `service.name == "checkout"`)
2. **Referenced metrics** — the metric fields used in aggregations (e.g. `request.duration`)
3. **Time range** — any temporal bounds from the query

It then resolves the `::exemplars` component of the referenced metrics data stream(s),
applies the extracted dimension filters and time range, and returns exemplar documents as the
source dataset for subsequent ES|QL processing (e.g. `| LIMIT 100`,
`| WHERE trace_id == "..."`, etc.).

Standard `_search` also works by targeting `metrics-otel-default::exemplars` directly.

This is semantically equivalent to PromQL's `/api/v1/query_exemplars` endpoint, which takes
a metric selector and returns exemplars matching those label matchers.

### Exemplar Document Schema (OTel)

Defined in OTel component templates. An exemplar document contains:

- **Dimension fields** (from shared component templates like `otel@mappings`):
  `resource.attributes.*`, `scope.name`, `attributes.*`, etc.
- **Required by index mode:** `@timestamp`
- **Exemplar-specific fields** (defined by the OTel exemplar template):
  - `trace_id` (keyword) — W3C trace identifier
  - `span_id` (keyword) — W3C span identifier
  - `value` (double) — the sampled metric value at this exemplar point
  - `filtered_attributes` (passthrough object) — additional key-value context
  - `_dimensions_hash` (keyword) — deterministic hash for sort/routing (implementation
    detail of the OTLP/Prometheus templates)

### Lifecycle

The exemplar component has its own `DataStreamLifecycle`, following the same pattern as the
failure store: configured in the template's exemplar store options, with a system default
applied when no explicit lifecycle is set. This means:

- If the template configures an exemplar lifecycle, that is used.
- If not, a default exemplar lifecycle is applied (e.g. 7 days retention).
- The exemplar lifecycle is independent from the `::data` lifecycle — short retention for
  exemplars does not affect metrics retention.

This mirrors `DataStream.getFailuresLifecycle()`, which returns the configured failure store
lifecycle or falls back to `DEFAULT_FAILURE_LIFECYCLE`.

### Security

Access to `::exemplars` follows the same authorization as `::data`. A role granting read
access to a metrics data stream implicitly grants read access to its exemplar component.

This deliberately diverges from the failure store model, where `::failures` has independent
privileges (`read_failure_store`, `manage_failure_store`). Exemplars are a read-side
companion to metrics — they share the same sensitivity boundary. Separate privileges would
add operational complexity without security benefit.

### Failure Handling

If an exemplar document fails to ingest, it is rejected (error returned to client). Exemplar
failures do not route to the failure store — exemplars are best-effort data.
We can later add a separate exemplar failure store if there is demand, but for now we keep the model simple.

### Rollover

The exemplar component rolls over independently from `::data`, based on its own conditions
(configured in lifecycle or triggered explicitly). This allows short-lived exemplar indices
without affecting metrics retention.
