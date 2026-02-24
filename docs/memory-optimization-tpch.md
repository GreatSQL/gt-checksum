# TPCH Large-Diff Memory Optimization (MySQL Path)

## 1. Baseline Problem Profile

In the TPCH large-diff scenario, memory spikes were caused by three stacked behaviors:

- Large chunk payloads were materialized as full strings and then split to slices.
- Diff tasks carried heavy payload objects through channels.
- Fix SQL generation buffered whole-table DELETE/INSERT SQL in memory before writing.

Observed symptom: with `memoryLimit=5120`, process memory could rise far beyond limit under heavy diff pressure.

## 2. Optimization Strategy

The optimization keeps checksum correctness and fix SQL semantic correctness, while changing memory behavior from linear-growth to bounded buffers.

### 2.1 Lightweight diff messages

- Remove heavy `SourceData`/`DestData` payload from diff channel messages.
- Keep only schema/table/where/column metadata in `DifferencesDataStruct`.
- Re-query chunk data on demand in `AbnormalDataDispos`, then release local large objects immediately.

### 2.2 Streaming fix SQL write with bounded batches

- Rewrite `DataFixDispos` to avoid full-table SQL aggregation.
- Maintain bounded `deleteBatch` / `insertBatch` with dual flush threshold:
  - SQL count threshold
  - approximate bytes threshold
- Flush batches incrementally via existing `processBatch` path.

### 2.3 Preserve DELETE-before-INSERT semantics

- `fixFilePerTable=ON`: write DELETE and INSERT to separate target files.
- `fixFilePerTable=OFF`: DELETE writes to final output stream; INSERT writes to a stage file.
- On completion, append INSERT stage file to final file to preserve global order.

### 2.4 Memory pressure backpressure

- Add memory pressure levels in monitor:
  - `70%`: freeze
  - `85%`: backpressure
  - `95%`: drain
- Expose producer wait API and queue depth adaptation API.
- Producers call `WaitForProducerAllowance()` before enqueuing heavy work.

### 2.5 Observability

- Add memory stage logs with:
  - `Alloc`
  - `HeapInuse`
  - `HeapObjects`
  - `NumGC`
  - per-table observed peak
- Add phase markers:
  - `table-start`
  - `chunk-query-start`
  - `diff-compare-start`
  - `fixsql-write-start`
  - `table-end`

## 3. Key Implementation Files

- `actions/table_query_concurrency.go`
  - shrink `DifferencesDataStruct` payload.
- `actions/table_index_dispos.go`
  - on-demand diff query handling
  - streaming `DataFixDispos`
  - memory stage logs and producer backpressure calls
- `utils/memory_monitor.go`
  - pressure levels
  - dynamic queue depth
  - producer gating API
  - memory snapshot API

## 4. Representative Code Snippets

### 4.1 Diff message payload reduction

```go
type DifferencesDataStruct struct {
    Schema          string
    Table           string
    TableColumnInfo global.TableAllColumnInfoS
    SqlWhere        map[string]string
    indexColumnType string
}
```

### 4.2 Streaming flush design

```go
if len(deleteBatch) >= batchFlushByCount || deleteBatchBytes >= batchFlushByApproxBytes {
    flushDeleteBatch()
}
if len(insertBatch) >= batchFlushByCount || insertBatchBytes >= batchFlushByApproxBytes {
    flushInsertBatch()
}
```

### 4.3 Memory pressure gate

```go
utils.WaitForProducerAllowance()
sourceSelectSql <- map[string]string{sp.sdrive: sourceSql}
```

## 5. Performance and Memory Validation Plan

Run before/after in same TPCH dataset and config (`memoryLimit=5120`).

Collect:

- Peak memory (MB)
- Total elapsed time (s)
- Check result rows/diffs
- fixsql total bytes

Acceptance targets:

- Peak memory <= 5632MB
- Check results unchanged
- fixsql semantic equivalence preserved
- Total time regression <= 15%

## 6. Monitoring Result Template

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Peak memory (MB) | TBD | TBD | TBD |
| Total time (s) | TBD | TBD | TBD |
| fixsql size (bytes) | TBD | TBD | TBD |
| Result consistency | pass/fail | pass/fail | - |

## 7. Risks and Notes

- SQL text ordering/shape may differ after streaming flush, but semantic execution result remains equivalent.
- Stage-file merge introduces extra I/O, bounded by sequential append.
- Oracle path is intentionally not deep-refactored in this change set.
