# Iceberg table sink

Append Arrow batches to an existing table using the Apache
Iceberg Rust SDK. Enable facade feature `sink-iceberg`, or depend directly on
`open-arrow-sink-iceberg`. The SDK is re-exported so callers can configure their
own catalog and use its `TableIdent` as the request.

`open` loads the table, checks that the session schema exactly matches
`iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())`, and
rejects read-only tables. Partitioned tables require `WriteOptions.partition` and
currently support only identity transforms. This includes Iceberg field-ID metadata
on Arrow fields; derive the input schema with the SDK helper above rather than
constructing metadata-free fields. Use `Ordering::Unordered`: Iceberg table
scans do not provide an input-row-order guarantee.

`write` streams batches through the SDK's Parquet/rolling data-file writer;
`commit` closes the files and publishes them with a native fast-append transaction.
Only file metadata accumulates until commit; rows are not collected in memory.
An empty session leaves the table unchanged.

Enable identity partition routing with the shared write option:

```rust
use open_arrow_sink_core::{Ordering, PartitionOptions, PartitionType, WriteOptions};

let options = WriteOptions {
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
spec. Do not repeat column names in the option. One or multiple
identity fields and null partition values are supported. Other transforms are
rejected at open. With partitioning disabled, partitioned tables remain rejected;
enabling it on an unpartitioned table keeps the unpartitioned writer path.

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

This first version has no table creation, non-identity partition transforms, schema evolution,
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
