# Usage

Start with the [complete Parquet application](../README.md#write-your-first-dataset).
Use Arrow 58.4-compatible batches and enable the destination feature you need.
Examples below assume the repository dependency from that quick start; switch to
a published version when the intended release is available.

## Select dependencies

```toml
[dependencies]
open-arrow-sink = { git = "https://github.com/dentiny/open-arrow-sink", default-features = false, features = ["parquet-zstd"] }
arrow-array = "58.4"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`parquet-zstd` also enables `sink-parquet`. For Lance choose `sink-lance`; for
Iceberg choose `sink-iceberg`. See [all feature mappings](support.md#features-and-storage).
Applications supply the async runtime; bundled examples use Tokio. Lance's native
build also requires `protoc`.

## Configure a destination

For local Parquet/Lance paths, convert an absolute directory path to the sink's
re-exported `Url`. Create needs a destination that does not exist; choosing an
already-created empty directory still fails for Parquet. Use a path without
`..` traversal segments; URL conversion does not normalize those segments for you.

```rust,ignore
use open_arrow_sink::parquet::Url;
let destination = Url::from_directory_path(std::path::absolute("output/run-001")?)
    .map_err(|_| "invalid directory")?;
```

For a cloud destination enable, for example, `parquet-aws` and supply an S3 URL
such as `s3://my-bucket/exports/run-001/`. For custom credentials/endpoints, or to
share an in-memory namespace, inject a configured `Storage`. Its operator root
must correspond to its base URL:

```rust,ignore
use open_arrow_sink::{parquet::ParquetDirectorySink, storage::{Storage, opendal}};

let operator = opendal::Operator::new(opendal::services::S3::default()
    .bucket("my-bucket")
    .root("exports/")
    .region("us-west-2"))?;
let storage = Storage::new(operator, "s3://my-bucket/exports/".parse()?)?;
let destination = storage.url("run-001/")?;
let sink = ParquetDirectorySink::new(storage);
```

Credentials are configured through OpenDAL/the application's environment; do not
embed them in destination URLs. Destination URLs reject queries and fragments;
HTTP download URLs have a separate policy and support signed queries. Storage
never silently falls back to another operator for out-of-root URLs. See
[shared storage](../storage/README.md) for retries, timeouts, HTTP limits, and upload buffers.

## Stream batches and end the session

A sink is reusable; each `open` owns an independent write session with a fixed
schema. `Writer::schema()` exposes that input schema. Keep every batch compatible,
including field metadata, nullability, and column order. Cast or reconcile upstream.

This function accepts any backend request and any synchronous iterator producing
batches. A streaming async source can use the same loop around its next-batch API.
Abort on an input error as well as a write error:

```rust,no_run
use open_arrow_sink::{RecordBatch, Result, Sink, WriteOptions, WriteSummary};

async fn write_batches<S: Sink>(
    sink: &S,
    request: S::Request,
    schema: open_arrow_sink::SchemaRef,
    options: WriteOptions,
    batches: impl IntoIterator<Item = Result<RecordBatch>>,
) -> Result<WriteSummary> {
    let mut writer = sink.open(request, schema, options).await?;
    for batch in batches {
        writer.write(batch?).await?;
    }
    writer.commit().await
}
```

`commit` and `abort` consume the writer. Successful `write` means accepted, not
published. A cancelled write is terminal too: abort or drop the writer rather
than continuing. Errors return directly without automatic abort, deletion, or
rollback. Explicit abort stops writing and leaves persisted output in place.
A commit error may have an uncertain outcome; reconcile with the backend before
starting another attempt. A successful summary reports committed rows, not files
or bytes.

## Recover from a failed commit

Write and commit failures return the error without deleting output, aborting
writers, or retrying. Drop also performs no asynchronous cleanup. Partial files,
fragments, and blobs can remain for application-managed maintenance.

`commit(self)` consumes the writer even on error. A write error or cancellation
makes the writer terminal; drop it or explicitly abort to stop pending I/O.
Publication may have succeeded despite a commit error.

After a commit error or cancellation:

1. Stop writes to that destination and retain the error, destination, and any
   known session paths or dataset/table versions for investigation.
2. Inspect the backend's current state to determine whether publication took
   effect. A timeout or other error can occur after publication succeeded. For
   Lance, also distinguish `IndexCommitError`: its data is already committed,
   and successful earlier indexes remain published. Do not replay those rows.
3. Remove only output confirmed to be unreferenced and owned by the failed
   session. For versioned datasets/tables, use backend-aware orphan maintenance
   that respects retained versions/snapshots and active writers. Do not delete
   a dataset/table prefix merely because `commit` returned an error.
4. Open a fresh session only after reconciling the destination. Retrying an
   Append without reconciliation can duplicate rows; Create can fail if orphan
   output still occupies the destination.

### What the next overwrite does

Overwrite is a new full replacement of the destination's current rows. It does
not resume the failed commit, provide deduplication, or automatically collect
leftovers from earlier sessions. Exclude concurrent destination mutations.

| Backend | Next Overwrite after a failed commit |
| --- | --- |
| Parquet | Unsupported; rejected before writing. |
| Lance | Requires an existing prefix to load as a valid dataset; a missing prefix can be created. A prefix containing only a failed Create's fragments can therefore block Overwrite until confirmed uncommitted leftovers are cleaned. For a valid dataset, success publishes a new manifest replacing the current rows while retaining historical versions/files; it does not purge prior orphan files. |
| Iceberg | Unsupported; rejected before writing. Reload/reconcile the table before opening another Append session. |

## Choose write options

```rust
use open_arrow_sink::{Compression, CompressionOptions, Ordering, WriteMode, WriteOptions};

let options = WriteOptions::default()
    .with_mode(WriteMode::Create)
    .with_ordering(Ordering::Unordered)
    .with_compression(CompressionOptions {
        global: Some(Compression::Zstd { level: Some(9) }),
        ..Default::default()
    })
    .with_target_file_size(256 * 1024 * 1024);
```

With Parquet this needs `parquet-zstd`. Empty compression options preserve backend
defaults; explicit `Uncompressed` disables compression. Per-column settings
replace the global codec and level, and use exact unique top-level field names.
File size is a positive soft target, not a strict maximum or a total-output limit.

Use `Append` only for existing destinations. `Overwrite` replaces all rows, not
just affected partitions; Parquet and Iceberg do not support it. Lance
retains historical dataset versions. Inspect capabilities and read the backend's
publication rules before selecting a mode.

## Write an Iceberg table

Configure a catalog and create a table outside the sink. Derive the input schema
from table metadata so it contains the required Iceberg field IDs:

```rust,ignore
use open_arrow_sink::{
    Ordering, Sink, WriteMode, WriteOptions,
    iceberg::{IcebergTableSink, iceberg},
};
use std::sync::Arc;

// catalog: Arc<dyn iceberg::Catalog>; identifier: iceberg::TableIdent
let table = catalog.load_table(&identifier).await?;
let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
    table.metadata().current_schema(),
)?);
let sink = IcebergTableSink::new(catalog);
let mut writer = sink
    .open(
        identifier,
        schema,
        WriteOptions::default()
            .with_mode(WriteMode::Append)
            .with_ordering(Ordering::Unordered),
    )
    .await?;
// Build batches with this exact schema, write, then commit.
```

For a partitioned table, set `partition: Some(PartitionOptions { partition_type:
PartitionType::Day })` for an existing `day(...)` spec, for example. Every field
must use the requested category. Bucket counts and truncate widths come from the
table spec. This option enables routing and does not create/change the spec.
See [Iceberg](../sink/iceberg/README.md) for all transforms and storage injection.

## Write Lance and build indexes

Construct `LanceDatasetSink::default()` with a URL, or `new(storage)` for a shared
operator. Select unordered writes and a supported mode. Add `IndexSpec` entries
to `WriteOptions.indexes` for supported column types. Index construction runs
after the data commit and can publish additional versions; `IndexCommitError`
in `Error::IndexCommit` means data is already committed. Do not replay those rows
in response to an index error.

Use `sink.dataset_builder(url)?.load().await?` to read with the same storage.
[The Lance guide](../sink/lance/README.md) explains vector training requirements,
effective-schema matching, native blobs, and explicit file versions `2.2`/`2.3`.
The sink's built-in [BlobDownloader](options.md#blob-downloader-and-memory-limits)
materializes logical blob URI inputs automatically. Configure its shared resource
limits; Lance chooses the resulting storage layout.

## Runnable examples

Run from the repository root. Choose fresh output paths for Create examples.

```sh
cargo run --locked -p open-arrow-sink-parquet --example write_directory -- /tmp/example-parquet
cargo run --locked -p open-arrow-sink-iceberg --example append
cargo run --locked -p open-arrow-sink-lance --example write_lance -- /tmp/example.lance
cargo run --locked -p open-arrow-sink-lance --example write_indexed -- /tmp/example-indexed.lance
cargo run --locked -p open-arrow-sink-layer --example custom_sink
cargo run --locked -p open-arrow-sink-layer --example custom_layer
```

The Iceberg example creates an ephemeral catalog/table outside the sink, appends,
and scans the result. The Lance example writes and checks readback. Extension
examples require no destination credentials and make no persistent writes.
