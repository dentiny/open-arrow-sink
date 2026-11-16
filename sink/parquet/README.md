# Parquet directory sink

`ParquetDirectorySink` accepts a destination `Url` for Create, Append, or filesystem staged Overwrite and, with default features, writes uncompressed
files named `part-00000.parquet`, `part-00001.parquet`, etc. By default it writes
one file per session. Missing parents are created. The default Create mode
rejects an existing target, including an empty local directory. Enable facade feature `sink-parquet`
and import `open_arrow_sink::parquet::ParquetDirectorySink`, or use this crate directly.


## Shared storage

The request for both single and parallel sessions is `Url`, re-exported from this
crate. Use `ParquetDirectorySink::default()` for URL-resolved storage, or
`ParquetDirectorySink::new(storage)` to share a configured OpenDAL operator:

```rust,ignore
use open_arrow_sink_core::Sink;
use open_arrow_sink_parquet::{ParquetDirectorySink, Storage, Url};
use open_arrow_sink_storage::opendal;

let operator = opendal::Operator::new(opendal::services::Memory::default())?;
let storage = Storage::new(operator, "memory:///warehouse/".parse()?)?;
let sink = ParquetDirectorySink::new(storage.clone());
let destination = storage.url("new-output/")?;
let writer = sink.open(destination, schema, options).await?;
```

Convert existing paths with `Url::from_directory_path(std::path::absolute(path)?)`.
Enable sink features `aws`, `gcp`, or `azure` (facade: `parquet-aws`, `parquet-gcp`,
`parquet-azure`) for URL-resolved cloud storage. For credentials, endpoints, and
layers, pass a configured `Storage`; see [shared storage](../../storage/README.md).

Create and staged Overwrite reserve a new prefix with conditional creation of `.open-arrow-sink`.
It also claims each part name before opening a streaming writer. Backends without
conditional-create support fail instead of overwriting. External writers must
not modify a reserved prefix. The marker is removed after a file finishes; all
part names have already been claimed, so the destination remains occupied.
Abort removes owned parts and the marker when no other objects remain, preserving
unrelated files. Drop performs no async cleanup. These rules apply to local and
remote destinations alike.

Use `WriteOptions { ordering: Ordering::Unordered, ..Default::default() }`: this
sink does not promise directory scan order. Each write validates its schema. Small
batches share Parquet row groups instead of forcing one row group per write.
Parquet buffers the current row group up to its default row limit (1,048,576 rows)
and keeps metadata for completed groups. Memory depends on row width, dictionaries,
the incoming batch and encoding buffers; this is not a fixed byte budget.

Set `WriteOptions.target_file_size` to a positive byte count to roll files between
nonempty batches once the current file's encoded bytes reach the target. A batch
and the file footer can exceed the target; it is not a strict maximum. `None`
keeps the single-file behavior. Buffered encoded-size estimates trigger an early
row-group flush near an explicit target; actual flushed bytes decide whether to
roll at the next nonempty batch. An empty session still creates one readable file.

Commit completes the final file footer. The target is visible while writing and has no
atomic directory transaction or durability/fsync guarantee.

`WriteOptions.cleanup_on_error` defaults to `true`: abort removes this session's files and
attempts to remove its directory; failed open/commit also attempts cleanup. Set it
to `false` to retain session-created output on abort or failure. Cleanup never
recursively deletes other files. Drop leaves incomplete output and never commits.

No partitioning. Storage retries are configured on the OpenDAL operator.

For concurrent files, wrap the sink in
`UnorderedWriteLayer::new(NonZeroUsize::new(4).unwrap())`. The layer calls
`ParallelSink::open_writers` to reserve the directory once and pre-open four
files. Each writer rolls independently using disjoint indices (writer 0 owns
0, 4, 8, ...; writer 1 owns 1, 5, 9, ...). The target size applies to each file.
This overlaps file I/O; Parquet encoding and compression still run in the layer
caller's task. Extra writers do not provide CPU parallelism and each retains its
own encoding buffers. See the [layer guide](../../layer/README.md#concurrent-writes)
for using `ParallelSink` directly with application-managed worker tasks.
Empty sessions produce one readable empty file per pre-opened writer. Batch
distribution and directory scan order are unspecified. Data writers finish all
footers before the session committer returns the combined row count. A finish
failure aborts every writer according to the cleanup policy. Files remain visible
before commit; there is no atomic directory transaction. See the [layer guide](../../layer/README.md) for
buffering, cancellation, partial commit, and layer composition semantics.

```sh
cargo run -p open-arrow-sink-parquet --example write_directory -- /tmp/new-parquet-directory
```

## Index requests

This sink declares `SinkCapability.supported_indexes = &[]`. A nonempty
`WriteOptions.indexes` list returns `Error::Unsupported` before opening a writer
or accessing the destination. The default empty list keeps existing behavior.

## Compression

Supports global compression and per-column overrides through `WriteOptions.compression`
or `CompressionLayer`. Uncompressed is always available. Other codecs require
opt-in Cargo features (none are enabled by default):

| Codec | Sink crate feature | Facade feature |
| --- | --- | --- |
| Snappy | `snappy` | `parquet-snappy` |
| Gzip | `gzip` | `parquet-gzip` |
| Zstd | `zstd` | `parquet-zstd` |
| Lz4Raw | `lz4` | `parquet-lz4` |

For example, enable `parquet-zstd` on `open-arrow-sink`, or `zstd` on
`open-arrow-sink-parquet`. Facade codec features also enable `sink-parquet`.
Capabilities advertise only enabled codecs; disabled codec requests return
`Error::Unsupported` before creating the destination. Cargo features are additive,
so other dependencies may still bring in codecs through their own Parquet features.

Column keys are exact top-level Arrow names; nested fields apply to all descendant columns.
Overrides and global settings persist across file rolling.

Explicit Zstandard levels use `Compression::Zstd { level: Some(level) }`.
Parquet accepts levels 1 through 22; unsupported levels are rejected before
creating the destination. `level: None` uses the Parquet default.

## Write modes

Create rejects existing destinations. Append validates the schemas of existing
root-level Parquet files and writes UUID-prefixed parts, preserving old files and
unrelated objects on abort. Schema validation uses range reads of Parquet metadata.
These modes work through the shared filesystem or remote operator.

Overwrite is staged on filesystem backends: old output stays visible until commit,
then the directory is replaced via backup/rename with restoration on failure.
The implementation uses OpenDAL for directory renames and cleanup; a local-only
`symlink_metadata` validation rejects symlink targets because
OpenDAL stat follows links. Other backends reject Overwrite before creating output;
object-store prefix swaps do not provide the directory-rename semantics used here.
External writers must not modify a destination during a session.

## Related documentation

[Usage](../../docs/usage.md) · [Architecture](../../docs/architecture.md) ·
[Support matrix](../../docs/support.md) · [Write layers](../../layer/README.md) ·
[Extend a sink](../../docs/extending-sinks.md)
