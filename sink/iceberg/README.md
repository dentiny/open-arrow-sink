# Iceberg table sink

Append Arrow batches to an existing table using the Apache
Iceberg Rust SDK. Enable facade feature `sink-iceberg`, or depend directly on
`open-arrow-sink-iceberg`. The SDK is re-exported so callers can configure their
own catalog and use its `TableIdent` as the request.

`open` loads the table, checks that the session schema exactly matches
`iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())`, and
rejects read-only tables. Partitioned tables require `WriteOptions.partition` and
support `Identity`, `Year`, `Month`, `Day`, `Hour`, `Bucket`, `Truncate`, and
`Void` transforms. Exact schema
matching includes Iceberg field-ID metadata on Arrow fields; derive the input
schema with the SDK helper above rather than
constructing metadata-free fields. Use `Ordering::Unordered`: Iceberg table
scans do not provide an input-row-order guarantee.

`write` streams batches through the SDK's Parquet/rolling data-file writer;
`commit` closes the files and publishes them with a native fast-append transaction.
Only file metadata accumulates until commit; rows are not collected in memory.
An empty session leaves the table unchanged.

Enable identity partition routing with the shared write option:

```rust
use open_arrow_sink_core::{
    Ordering, PartitionOptions, PartitionType, WriteMode, WriteOptions,
};

let options = WriteOptions::default()
    .with_mode(WriteMode::Append)
    .with_ordering(Ordering::Unordered)
    .with_partition(PartitionOptions {
        partition_type: PartitionType::Identity,
    });
```

The table's default partition spec and current schema are fixed when the session
opens. `partition_type` requests a transform type (default `Identity`); it must
be advertised by the sink and match the table spec. It does not change the table
spec. Do not repeat column names in the option. One or multiple partition fields
and null partition values are supported. Every field must use the requested
transform category; mixed categories are rejected at open.

| Category | Example table transform | Example grouping |
| --- | --- | --- |
| `Identity` | `identity(country)` | `CN` stays `CN` |
| `Year` | `year(created_at)` | timestamps in 2026 share a partition |
| `Month` | `month(created_at)` | timestamps in November 2026 share a partition |
| `Day` | `day(created_at)` | timestamps on 2026-11-14 share a partition |
| `Hour` | `hour(created_at)` | timestamps during the same hour share a partition |
| `Bucket` | `bucket(16, user_id)` | hashes each value into one of 16 buckets |
| `Truncate` | `truncate(10, amount)` | 21 and 29 both map to 20 |
| `Void` | `void(old_partition)` | every value maps to null |

`Year`, `Month`, and `Day` accept date or timestamp fields; `Hour` requires a
timestamp. Transform calculation and partition metadata are delegated to the SDK.

For example, use `partition_type: PartitionType::Day` for a table partitioned by
`day(created_at)`. For `Bucket` or `Truncate`, the existing table spec supplies
bucket counts or widths; do not repeat these parameters in `PartitionOptions`.
Multiple fields may use different parameters within the same transform category.
Bucket counts must be between 1 and `i32::MAX` (the SDK's divisor range), and
truncate widths must be positive. String truncation keeps the first N Unicode
code points. Unknown transforms remain rejected at open.

An all-void spec uses the SDK's unpartitioned writer path, with a null partition
value for every spec field retained in file metadata. It still requires
`partition_type: PartitionType::Void`. With partitioning disabled, tables with
partition fields remain rejected; enabling it on an unpartitioned table keeps the
unpartitioned writer path.

An internal adapter delegates partition calculation/batch splitting to the SDK's
`RecordBatchPartitionSplitter` and routing to its `FanoutWriter`. Input can mix
partitions in any order. The SDK keeps a writer for each distinct partition
until close; the adapter adds no writer-count limit or eviction. Splitting uses
temporary memory proportional to the input batch; each active writer has its
own Parquet buffers. File metadata also accumulates until commit.

All partition writers close before a single fast-append transaction publishes
the files. No partition is committed independently. Empty batches create no
partition writers, and an empty session does not publish a snapshot.

`WriteOptions.target_file_size` sets the SDK rolling writer's target in bytes.
`None` keeps the SDK default. This is a soft target checked between batches;
row-group buffering and file footers may make actual files larger.

`abort` drops the file writer and retains all persisted output. Write and commit
failures return directly without automatic abort or deletion.
Drop and failed/cancelled commits can leave orphan files. Commit
errors may have an unknown publication outcome: reload/reconcile the table before
retrying; files are deliberately retained on failure to avoid deleting committed
data. SDK catalog commit behavior (including its retry policy) applies.

This sink has no table creation, mixed partition transform categories, schema evolution,
overwrite, upsert or sorting.

```sh
cargo run -p open-arrow-sink-iceberg --example append
```

The example creates an ephemeral in-memory table outside the sink, appends three
rows, and reads them through the Iceberg scanner.

## Index requests

This sink declares `SinkCapability.supported_indexes = &[]`. A nonempty
`WriteOptions.indexes` list returns `Error::Unsupported` before opening a writer
or accessing the destination. The default empty list keeps existing behavior.

## Compression

Supports global data-file compression through `WriteOptions.compression` or
`CompressionLayer`: Uncompressed, Snappy, Gzip, Zstd, Lz4Raw. Overrides apply only
to this session, retaining other table writer settings. Column overrides are
unsupported and rejected before catalog access. Empty options retain table defaults.

Explicit Zstandard levels use `CompressionSpec::Zstd { level: Some(level) }`.
Levels 1 through 22 are supported; unsupported levels are rejected before catalog
access. `level: None` uses the codec default instead of the table's previous level.

## Concurrent writing

Wrap `IcebergTableSink` in `UnorderedWriteLayer::new(concurrency)` and explicitly
allow `Ordering::Unordered`. The sink loads and validates the table once and
creates independent Parquet writers with unique file names. Each writer honors
the target file size and destination partition spec. A writer can produce
multiple files and, when partitioned, multiple partition writers.

The layer overlaps I/O but polls all writers in the caller's task, so Parquet
encoding and compression do not run across CPU cores merely by increasing this
concurrency. A multithreaded runtime alone does not change that. See the
[layer guide](../../layer/README.md#concurrent-writes) for application-managed
`ParallelSink` worker tasks and their shutdown responsibilities.

Finishing a data writer returns its `DataFile` metadata without modifying the
catalog. The session committer combines all files in one fast-append transaction,
so there is no per-worker table commit. An empty session publishes no snapshot.
Before publication, failures return directly without aborting writers or deleting files.
After publication starts, errors retain files because the catalog may already
reference them. Per-writer encoding/fanout buffers multiply with concurrency.

## Shared OpenDAL storage

Inject `OpenDalStorageFactory` when building the catalog. The sink already uses
that catalog's table `FileIO`, so data files, manifest files, and scans
all use the same shared operator. The sink request remains `TableIdent`;
the table metadata determines data locations.

```rust,ignore
use iceberg::{CatalogBuilder, memory::MemoryCatalogBuilder};
use open_arrow_sink_iceberg::{IcebergTableSink, OpenDalStorageFactory, Storage, iceberg};
use open_arrow_sink_storage::opendal;
use std::sync::Arc;

let operator = opendal::Operator::new(opendal::services::Memory::default())?;
let storage = Storage::new(operator, "memory:///".parse()?)?;
let factory = Arc::new(OpenDalStorageFactory::new(storage));
let catalog = MemoryCatalogBuilder::default()
    .with_storage_factory(factory)
    .load("example", catalog_properties)
    .await?;
let sink = IcebergTableSink::new(Arc::new(catalog));
```

Use a configured filesystem/cloud operator and matching base URL for other
backends. The base must cover every location used by the table, including any
custom data location. Out-of-root URLs fail rather than falling back to another
backend. Existing caller-configured catalogs continue to work unchanged.
`OpenDalStorageFactory::file_io()` also builds a `FileIO` for native/static-table
APIs. Factories retain live operators and deliberately reject FileIO serialization;
reconstruct them with the configured operator in another process.

Native Iceberg owns publication and commit-failure handling. This adapter does
not change those semantics or introduce a second table transaction. Iceberg's
`FileWrite` interface has no asynchronous abort method. The factory releases
unfinished upload handles with their writers and does not retain them for error
cleanup. Drop performs no asynchronous work. Applications own any later orphan
file or unfinished-upload maintenance.

Per-session `WriteOptions.upload.part_size` and `WriteOptions.upload.concurrency` overrides are
unsupported because the catalog supplies FileIO. Configure
`Storage::with_upload_part_size` and `Storage::with_upload_concurrency` before
constructing `OpenDalStorageFactory` instead.

### Supported write modes

`IcebergTableSink.capability().supported_modes` is `[WriteMode::Append]`.
`options.with_mode(WriteMode::Append)` appends to an existing
table. A missing table is an error. The shared default is `WriteMode::Create`,
which this sink rejects; callers must explicitly select Append.

Create and Overwrite return `Error::Unsupported` before catalog access, leaving
existing snapshots and data unchanged. The iceberg-rust 0.11 dependency exposes
fast append but no public full-overwrite transaction, and `TableIdent` does not
carry table creation settings. This sink never drops/recreates a table to emulate
overwrite. Callers can inspect capabilities to choose an appropriate sink or mode.

## Related documentation

[Usage](../../docs/usage.md) · [Architecture](../../docs/architecture.md) ·
[Support matrix](../../docs/support.md) · [Write layers](../../layer/README.md) ·
[Extend a sink](../../docs/extending-sinks.md)
