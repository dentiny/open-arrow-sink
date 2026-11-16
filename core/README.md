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
`Error::Unsupported` covers rejected requests, `SchemaMismatch` covers batch
schemas, and `Backend` retains an error source. `IndexCommitError`, wrapped in
`Backend`, identifies index construction failure after data publication.

See [architecture](../docs/architecture.md), [options](../docs/options.md),
[implementing sinks](../docs/extending-sinks.md), and
[implementing layers](../docs/extending-layers.md) for lifecycle requirements.

### Publication outcome on errors

`Error::publication_outcome()` returns typed `PublicationOutcome` context:
`NotPublished` means a transactional backend guarantees this session published no
metadata; `Published` means data committed and a later operation (such as index
construction or reservation cleanup) failed; `Unknown` requires checking the
actual destination before retrying. Unclassified errors and third-party sinks
conservatively return `Unknown`. Error context retains the original source chain;
postcommit `IndexCommitError` is still directly downcastable from `Error::Backend`.

This is not an automatic retry decision. Even `NotPublished` can leave reservations
and orphan files, and retrying Create may need cleanup or a different mode. Parquet
Create/Append files are visible before commit, so their precommit errors remain
`Unknown`. An error-free result does not arise on cancellation: cancelling commit
can lose its result, and its publication outcome must also be treated as unknown.
