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
use open_arrow_sink_core::{Ordering, PartitionOptions, PartitionType, WriteMode, WriteOptions};

let options = WriteOptions {
    mode: WriteMode::Append,
    ordering: Ordering::Unordered,
    partition: Some(PartitionOptions {
        partition_type: PartitionType::Identity,
    }),
    ..WriteOptions::default()
};
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

`abort` drops the file writer and attempts to delete only that session's unique
data prefix. Drop and failed/cancelled commits can leave orphan files. Commit
errors may have an unknown publication outcome: reload/reconcile the table before
retrying; files are deliberately retained on failure to avoid deleting committed
data. SDK catalog commit behavior (including its retry policy) applies.

This sink has no table creation, mixed partition transform categories, schema evolution,
overwrite, upsert, sorting or concurrency settings.

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

Explicit Zstandard levels use `Compression::Zstd { level: Some(level) }`.
Levels 1 through 22 are supported; unsupported levels are rejected before catalog
access. `level: None` uses the codec default instead of the table's previous level.

### Supported write modes

`IcebergTableSink.capability().supported_modes` is `[WriteMode::Append]`.
`WriteOptions { mode: WriteMode::Append, ..options }` appends to an existing
table. A missing table is an error. The shared default is `WriteMode::Create`,
which this sink rejects; callers must explicitly select Append.

Create and Overwrite return `Error::Unsupported` before catalog access, leaving
existing snapshots and data unchanged. The iceberg-rust 0.11 dependency exposes
fast append but no public full-overwrite transaction, and `TableIdent` does not
carry table creation settings. This sink never drops/recreates a table to emulate
overwrite. Callers can inspect capabilities to choose an appropriate sink or mode.
