# Parquet directory sink

`ParquetDirectorySink` accepts a new local directory path and writes one
uncompressed `part-00000.parquet` file. The parent must exist; an existing target
is rejected, including an empty directory. Enable facade feature `sink-parquet`
and import `open_arrow_sink::parquet::ParquetDirectorySink`, or use this crate directly.

Use `WriteOptions { ordering: Ordering::Unordered, ..Default::default() }`: this
sink does not promise directory scan order. Each write validates its schema and flushes its rows; it
does not retain all input batches. Parquet still keeps row-group metadata and
needs memory proportional to the incoming batch and encoding buffers.

Commit completes the file footer. The target is visible while writing and has no
atomic directory transaction or durability/fsync guarantee.

`WriteOptions.cleanup_on_error` defaults to `true`: abort removes this session's file and
attempts to remove its directory; failed open/commit also attempts cleanup. Set it
to `false` to retain session-created output on abort or failure. Cleanup never
recursively deletes other files. Drop leaves incomplete output and never commits.

No append, overwrite, partitioning, compression options, cloud storage or retries.

```sh
cargo run -p open-arrow-sink-parquet --example write_directory -- /tmp/new-parquet-directory
```
