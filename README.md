# open-arrow-sink

A Rust library for writing Apache Arrow `RecordBatch` values to Parquet
directories, Iceberg tables, and Lance datasets. Use it as the output stage of
a data preparation or ingestion pipeline.

[open-arrow-access](https://github.com/dentiny/open-arrow-access) is the upstream
read side: it discovers and decodes source data into Arrow batches. Your pipeline
processes those batches, then passes them to open-arrow-sink for writing. The two
libraries have no compile-time dependency; any compatible Arrow producer works.

```text
Sources → open-arrow-access → Arrow batches → your processing → open-arrow-sink → datasets
```

## Architecture

```mermaid
flowchart LR
    App["Application: Arrow batches"] --> Layers["Optional layers"]
    Layers --> Sink["Sink: open a write session"]
    Sink --> Writers["DataWriter(s): encode output"]
    Writers --> Storage["Storage: OpenDAL byte I/O"]
    Writers --> Committer["SessionCommitter: publish outputs"]
    Storage --> Destination["Parquet / Iceberg / Lance"]
    Committer --> Destination
```

| Component | Responsibility | Code |
| --- | --- | --- |
| Core | Sink, writer, session, layer, options, and capability contracts | [core/src](core/src) |
| Layers | Compression policy, concurrent writes, metrics, schema adaptation, URL-to-blob selection | [layer/src](layer/src) |
| Storage | OpenDAL operators, credentials, byte streams, HTTP and download budgets | [storage/src](storage/src) |
| Sinks | Destination validation, encoding, and backend-specific publication | [sink](sink) |
| Facade | Re-exports and opt-in backend features | [src/lib.rs](src/lib.rs) |

Core is independent of storage and backends. Layers and backends build on these
contracts; the application owns the runtime and input pipeline.

A reusable `Sink` opens an owned `Writer` with a fixed schema. The lifecycle is
`open → write batches → commit` or `abort`. Writes accept data; commit finishes
writers and publishes their outputs through one session committer. Drop never
commits or performs async cleanup. Failed or cancelled writes make the writer
terminal; abort stops I/O and retains persisted output. A commit error can leave
publication uncertain, so check the destination before retrying.

For a complete input stream, import `SinkExt` and let the sink own the loop:

```rust,ignore
use open_arrow_sink::{SinkExt, WriteOptions};

let summary = sink
    .write_all(destination, schema, WriteOptions::default(), stream)
    .await?;
```

`stream` can be any `Send` stream of `open_arrow_sink::Result<RecordBatch>`,
including an unpinned async stream. Map errors from other libraries at the input
boundary with `stream.map_err(open_arrow_sink::Error::backend)` (using
`futures::TryStreamExt`); this preserves the native error and its source chain. The helper opens once, writes
one batch at a time, and commits at EOF, even for empty input. Input or write
errors trigger best-effort abort and return the original `Error` even if abort
also fails.
Open errors return before reading input. Commit errors return directly because
commit consumes the writer and may have already published data. Cancelling the
future or panicking drops the writer without async cleanup.

`write_all` returns the backend's `CommitSummary`, including actual committed
rows after filtering and blob recovery.
See the [Parquet example](sink/parquet/examples/write_directory.rs) for a runnable
use of `write_all`.

`Layer<S>` wraps a sink while preserving its request type.
`sink.layer(a).layer(b)` runs through `b → a → sink`.
`UnorderedWriteLayer` uses `ParallelSink` to create several data writers with one
committer. Arrow layers control writes; OpenDAL layers control byte I/O.

## Backends and examples

| Backend | Facade feature | Modes | Publication | Example |
| --- | --- | --- | --- | --- |
| Parquet directory | `sink-parquet` | Create, Append | Files may be visible before commit; no directory transaction | [write_directory.rs](sink/parquet/examples/write_directory.rs) |
| Iceberg table | `sink-iceberg` | Append to an existing table | One native fast-append transaction | [append.rs](sink/iceberg/examples/append.rs) |
| Lance dataset | `sink-lance` | Create, Append, Overwrite | One atomic manifest for data and requested indexes | [write_lance.rs](sink/lance/examples/write_lance.rs) |

Facade features default to empty. Enable the backend you need; see
[Cargo.toml](Cargo.toml) for cloud and codec features. Options default to Create
and Unordered; Iceberg requires explicit Append. Bundled sinks reject
PreserveInput. Check `Sink::capability()` and backend validation for supported
options.

Run the Parquet example with a new destination directory:

```sh
cargo run --locked -p open-arrow-sink-parquet --example write_directory -- /tmp/my-first-arrow-output
```

For integration details, start with the examples above and the
[write options](core/src/request.rs). Generate API documentation locally:

```sh
cargo doc --workspace --all-features --no-deps --locked --open
```

For S3 blob downloads, a single `md5=<32 hexadecimal digits>` query (as in PMC
content URLs) is stripped before resolving source storage. The payload checksum
is not verified. Other storage queries remain unsupported; HTTP(S) queries are
preserved. For the public PMC bucket, register an unsigned S3 operator configured
with `region("us-east-1")` and `skip_signature()` via `with_source_storage`.

## Extending and developing

Implement a **sink** for a new destination, or a **layer** for reusable policy or
batch transformation. Read [Extending](docs/extending.md) for the contracts and
runnable examples. Extensions can live in separate crates without registration.

Use a Rust toolchain supporting edition 2024. Lance dependencies also need
`protoc`. Run the workspace checks from the repository root:

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

MIT licensed.

## Commit results

`Writer::commit()`, `SessionCommitter::commit(outputs)`, and
`SinkExt::write_all()` return a `CommitSummary` for the successfully published session. `rows_committed` counts
actual output rows across all physical writers, including when blob recovery
skips input rows. `rows_failed_blobs` counts each input row with a failed selected
blob once, even if several blobs fail; it overlaps `rows_skipped` under `SkipRow`
and includes retained rows under `Null`. These counters are local to the session;
`DownloadBlobLayer::metrics()` remains a cumulative observation handle.

```rust,ignore
let summary = writer.commit().await?;
progress.finish(summary.rows_committed, summary.rows_failed_blobs);
```

Empty sessions report zero rows; errors
return no summary and do not imply publication was rolled back. Custom writers,
committers, and decorators must return or forward the summary.
