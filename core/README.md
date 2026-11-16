# Core write contracts

`open-arrow-sink-core` contains the backend-independent interfaces. The facade
re-exports these types, including `RecordBatch`, `SchemaRef`, and `async_trait`.
Use this crate directly when implementing a backend without the facade's layers
and storage dependencies.

| Contract | Responsibility |
| --- | --- |
| `Sink` | Reusable, `Send + Sync` configuration; typed `Request: Send`; capability and async open |
| `Writer` | Owned `Send` session; fixed schema; bounded writes; consuming commit/abort |
| `ParallelSink` | Exactly N schema-compatible data writers and one publication authority |
| `DataWriter` | Physical output; `finish(&mut self)` returns metadata; abort remains possible after finish |
| `SessionCommitter<T>` | Publish every output once, or clean up session resources after writer aborts |
| `Layer<S>` / `SinkExt` | Construct decorators without I/O, preserving the request type |
| `BlobWriter` / `BlobHandle` / `BlobPlacement` | Stream bytes and attach completed session-owned native blob descriptors |

`validation::validate_options` checks shared capabilities, compression names,
positive file sizes, modes, and ordering before destination I/O. It does not
validate backend codec levels, index column types, version syntax, or table
partition specs. Index-capable implementations also call `validate_indexes`.
`validate_batch` enforces exact schema equality. `unique_column_index` resolves
one exact top-level name; dots are literal and duplicate names are ambiguous.

`WriteSummary.rows_written` is the successful session's committed row count.
`Error::Unsupported` covers unavailable capabilities; `InvalidConfig` covers
malformed options and `InvalidInput` covers malformed URLs, paths, and blob
placements. `AlreadyExists` reports a destination or index conflict with context.
`NotFound` reports a missing destination detected by validation, and `SchemaMismatch`
covers batch schemas. Native I/O failures remain in `Backend`, which retains
the original source chain and backend context.

`Error::IndexCommit(IndexCommitError)` identifies index construction failure
after data publication and retains the successful data write in `summary`.
Match this variant directly; retry indexing rather than replaying the write.

`Error`, `WriteOptions`, `SinkCapability`, and `CompressionCapability` are
non-exhaustive so new errors and options can be added after release. Construct
options and capabilities with `Default` plus their `with_*` methods; public
fields can also be read and changed directly. Struct literals and update syntax
are unavailable outside the core crate. Error matches must include a fallback
arm. For example:

```rust
use open_arrow_sink_core::{Ordering, WriteMode, WriteOptions};

let options = WriteOptions::default()
    .with_mode(WriteMode::Append)
    .with_ordering(Ordering::Unordered);
```

See [architecture](../docs/architecture.md), [options](../docs/options.md),
[implementing sinks](../docs/extending-sinks.md), and
[implementing layers](../docs/extending-layers.md) for lifecycle requirements.
