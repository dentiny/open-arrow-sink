# Iceberg table sink

Append Arrow batches to an existing **unpartitioned** table using the Apache
Iceberg Rust SDK. Enable facade feature `sink-iceberg`, or depend directly on
`open-arrow-sink-iceberg`. The SDK is re-exported so callers can configure their
own catalog and use its `TableIdent` as the request.

`open` loads the table, checks that the session schema exactly matches
`iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())`, and
rejects partitioned or read-only tables. This includes Iceberg field-ID metadata
on Arrow fields; derive the input schema with the SDK helper above rather than
constructing metadata-free fields. Use `Ordering::Unordered`: Iceberg table
scans do not provide an input-row-order guarantee.

`write` streams batches through the SDK's Parquet/rolling data-file writer;
`commit` closes the files and publishes them with a native fast-append transaction.
Only file metadata accumulates until commit; rows are not collected in memory.
An empty session leaves the table unchanged.

`abort` drops the file writer and attempts to delete only that session's unique
data prefix. Drop and failed/cancelled commits can leave orphan files. Commit
errors may have an unknown publication outcome: reload/reconcile the table before
retrying; files are deliberately retained on failure to avoid deleting committed
data. SDK catalog commit behavior (including its retry policy) applies.

This first version has no table creation, partition routing, schema evolution,
overwrite, upsert, sorting or concurrency settings.

```sh
cargo run -p open-arrow-sink-iceberg --example append
```

The example creates an ephemeral in-memory table outside the sink, appends three
rows, and reads them through the Iceberg scanner.
