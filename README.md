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

Lance Overwrite can retry into empty directories or directories containing only
recognized uncommitted Lance files. Orphans are retained; unrelated contents and
invalid dataset metadata still fail. Create and Append are unchanged.

Run the Parquet example with a new destination directory:

```sh
cargo run --locked -p open-arrow-sink-parquet --example write_directory -- /tmp/my-first-arrow-output
```

For integration details, start with the examples above and the
[write options](core/src/request.rs). Generate API documentation locally:

```sh
cargo doc --workspace --all-features --no-deps --locked --open
```

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
