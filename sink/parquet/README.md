# Parquet directory sink

`ParquetDirectorySink` accepts a new local directory path and writes uncompressed
files named `part-00000.parquet`, `part-00001.parquet`, etc. By default it writes
one file per session. The parent must exist; an existing target
is rejected, including an empty directory. Enable facade feature `sink-parquet`
and import `open_arrow_sink::parquet::ParquetDirectorySink`, or use this crate directly.

Use `WriteOptions { ordering: Ordering::Unordered, ..Default::default() }`: this
sink does not promise directory scan order. Each write validates its schema and flushes its rows; it
does not retain all input batches. Parquet still keeps row-group metadata and
needs memory proportional to the incoming batch and encoding buffers.

Set `WriteOptions.target_file_size` to a positive byte count to roll files between
nonempty batches once the current file's encoded bytes reach the target. A batch
and the file footer can exceed the target; it is not a strict maximum. `None`
keeps the single-file behavior. An empty session still creates one readable file.

Commit completes the final file footer. The target is visible while writing and has no
atomic directory transaction or durability/fsync guarantee.

`WriteOptions.cleanup_on_error` defaults to `true`: abort removes this session's files and
attempts to remove its directory; failed open/commit also attempts cleanup. Set it
to `false` to retain session-created output on abort or failure. Cleanup never
recursively deletes other files. Drop leaves incomplete output and never commits.

No append, overwrite, partitioning, compression options, cloud storage or retries.

```sh
cargo run -p open-arrow-sink-parquet --example write_directory -- /tmp/new-parquet-directory
```

## Index requests

This sink declares `SinkCapability.supported_indexes = &[]`. A nonempty
`WriteOptions.indexes` list returns `Error::Unsupported` before opening a writer
or accessing the destination. The default empty list keeps existing behavior.

## Compression

Supports global compression and per-column overrides through `WriteOptions.compression`
or `CompressionLayer`. Codecs: Uncompressed, Snappy, Gzip, Zstd, Lz4Raw. Column
keys are exact top-level Arrow names; nested fields apply to all descendant columns.
Overrides and global settings persist across file rolling.
