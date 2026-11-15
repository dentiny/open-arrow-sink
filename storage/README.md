# Shared storage

`open-arrow-sink-storage` owns byte I/O through OpenDAL. It has no dependency on
Parquet, Lance, Iceberg, or open-arrow-access. Sink crates own encoding, file
naming, destination reservation, and transaction publication.

A `Storage` binds a configured OpenDAL `Operator` to a base URL. The operator's
root must match that URL. Clones share the operator, including caller-configured
credentials, endpoints, retry, timeout, and other OpenDAL layers.

```rust
use open_arrow_sink_storage::{Storage, opendal};

let operator = opendal::Operator::new(opendal::services::Memory::default())?;
let storage = Storage::new(operator, "memory:///warehouse/".parse()?)?;
let destination = storage.url("new-output/part-00000.parquet")?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`Storage::from_url(&url)` resolves an operator rooted at the URL's origin. Local
`file://` and ephemeral `memory://` are available by default. Each memory
resolution constructs a fresh store; retain a configured `Storage` to share data.
Enable storage features `aws`, `gcp`, or `azure` for S3, GCS, or Azure Blob URLs.
For custom authentication and layers, supply your own operator with `Storage::new`.
`StorageConfig::apply` optionally installs validated retry and timeout policies.

URL resolution decodes spaces and Unicode, checks endpoint/root boundaries, and
rejects query strings, fragments, embedded credentials, and path traversal.
Root-destructive operations are rejected. `create` uses OpenDAL's atomic
`if_not_exists` operation; unsupported backends fail instead of overwriting.

`StorageWriter` streams with a 5 MiB chunk and one ongoing upload per writer.
Call `close` to publish bytes or `abort` to cancel an active upload. Failed or
cancelled writes are terminal; explicit abort retains access to the upload.
Drop performs no async cleanup. Cleanup of already published objects and the
meaning of a successful commit belong to each sink.

Filesystem operators must leave OpenDAL's `atomic_write_dir` unset. In OpenDAL
0.59 its check-then-rename staging does not enforce conditional creation atomically.
The default filesystem resolver therefore uses non-staged writes. Abort drops
those file handles; the owning sink removes its already-visible files. This
keeps local reservation and concurrent manifest publication safe.

## Alignment with open-arrow-access

The shared policy implementation is ported from
[open-arrow-access/storage](https://github.com/dentiny/open-arrow-access/tree/bbd6284585dbf60f802bab1cbe13acfbb37ce8ae/storage).
`config.rs` and `http_limit.rs` are copied with only the core-crate namespace
changed. `StorageConfig::validate` and its policy error messages follow the same
implementation, kept in `validation.rs` and `error.rs`. `backend.rs` uses the
same configured-operator and transport-budget pattern as `OpenDalAccessor`.

Each storage instance defaults to 64 ongoing HTTP requests, shared by its clones.
Use `with_http_concurrency_limit(limit.clone())` to share a total budget across
separately constructed storage instances. `operator()` returns a cloned operator
wrapped with that same budget, including when a native sink SDK uses it.
`Storage::try_new` applies the copied retry/timeout configuration before creating
the storage binding. Errors retain operation/object context and their original
OpenDAL source through the sink core's existing backend-error contract.

The remaining additions support this repository's write contract: a URL-scoped
namespace, streaming close/abort, conditional creation, and owned-output cleanup.
The access repository's read-only `ByteReader` and `NamedFileAccessor` contracts
are not exposed as write APIs here. No dependency on open-arrow-access is added.
