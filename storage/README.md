# Shared storage

`open-arrow-sink-storage` owns byte I/O through OpenDAL. It has no dependency on
Parquet, Lance, Iceberg, or open-arrow-access. Sink crates own encoding, file
naming, destination preparation, and transaction publication.

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
Call `.with_config(StorageConfig)` after either constructor to install validated
retry and timeout policies. `StorageConfig::apply` remains available for bare
operators. Policies are opt-in; timeout runs inside retry so each attempt gets its
own deadline. Configure storage once before cloning; repeated `with_config` calls
add layers while preserving caller-supplied operator layers.

URL resolution decodes spaces and Unicode, checks endpoint/root boundaries, and
rejects query strings, fragments, embedded credentials, and path traversal.
Root-destructive operations are rejected. `create` uses OpenDAL's atomic
`if_not_exists` operation; unsupported backends fail instead of overwriting.

`StorageWriter` streams with a 4 MiB chunk and up to four concurrent upload parts per writer.
Call `close` to publish bytes or `abort` to cancel an active upload. Failed or
cancelled writes are terminal; explicit abort retains access to the upload.
Drop performs no async cleanup. Cleanup of already published objects and the
meaning of a successful commit belong to each sink.

Filesystem operators must leave OpenDAL's `atomic_write_dir` unset. In OpenDAL
0.59 its check-then-rename staging does not enforce conditional creation atomically.
The default filesystem resolver therefore uses non-staged writes. Abort drops
those file handles; the owning sink removes its already-visible files. This
keeps conditional file creation and concurrent manifest publication safe.

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
`Storage::with_config` applies retry/timeout configuration to an existing storage
binding, including one built by `from_url`. Errors retain operation/object context
and their original OpenDAL source through the sink core's existing backend-error contract.

The remaining additions support this repository's write contract: a URL-scoped
namespace, streaming close/abort, conditional creation, and owned-output cleanup.
The access repository's read-only `ByteReader` and `NamedFileAccessor` contracts
are not exposed as write APIs here. No dependency on open-arrow-access is added.

## Configure URL-derived stores

`StorageResolver::default().with_config(config)?` applies policies to URL-derived
stores, including existing cache entries. Explicit `with_storage` registrations
keep their own policies. Resolver clones share cached backends and HTTP budgets,
but changing one clone's config does not affect other clones or previously
resolved storage.

`ParquetDirectorySink::default().with_config(config)?` configures automatically
resolved destinations. The same method also adds policies to explicit storage
passed to `ParquetDirectorySink::new`.
`DownloadPool::with_config(config)` configures both HTTP downloads and lazily
resolved non-HTTP sources; explicitly registered source stores keep their policies.

```rust
use open_arrow_sink_storage::{RetryConfig, Storage, StorageConfig, TimeoutConfig, Url};

let url = Url::parse("file:///tmp/output/")?;
let storage = Storage::from_url(&url)?.with_config(StorageConfig {
    retry: Some(RetryConfig::default()),
    timeout: Some(TimeoutConfig::default()),
})?;
# let _ = storage;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Size the shared HTTP budget

The default 64 is a total request budget for each `Storage` and its clones,
including operators passed to native SDKs. Upload parts, reads, listings, and
manifest requests compete for it. A permit remains occupied until the response
body finishes or is dropped; the limit counts HTTP requests, not writers or
logical storage operations.

Size the budget together with writer and multipart concurrency. For example,
eight writers with eight upload parts in flight each can occupy all 64 slots.
Additional metadata or read requests then wait for capacity. Increasing only
the number of writers or upload tasks cannot exceed this aggregate limit.
One starting calculation is `writers × in-flight parts per writer`, plus
headroom for the workload's other requests. Measure throughput, memory, and
service throttling before increasing it; the limiter does not reserve separate
capacity for reads or metadata.

Configure the budget before cloning storage into sinks or native adapters:

```rust
use open_arrow_sink_storage::{HttpConcurrencyLimitLayer, Storage, opendal};

// An example workload budget, rather than a universal recommended default.
let budget = HttpConcurrencyLimitLayer::new(128)?;
let operator = opendal::Operator::new(opendal::services::Memory::default())?;
let storage = Storage::new(operator, "memory:///warehouse/".parse()?)?
    .with_http_concurrency_limit(budget.clone());
// Reuse budget.clone() when separately constructed stores should share this cap.
let sink_storage = storage.clone();
# let _ = sink_storage;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Separately constructed stores have independent budgets unless given clones of
the same limiter. Reconfiguring one `Storage` does not change previously created
clones, operators, or sinks. This transport budget also does not set a
`DownloadPool`'s stream concurrency or buffered-byte quota; those are separate
admission limits.

## Multipart uploads

`Storage::with_upload_part_size(NonZeroUsize)` sets the streaming upload buffer
size in bytes for shared storage writers and Lance's native multipart uploads.
For example, use `NonZeroUsize::new(16 * 1024 * 1024).unwrap()` for 16 MiB parts.
OpenDAL enforces service-specific part-size limits. All upload paths now default
to 4 MiB parts and four concurrent requests per writer. Use
`Storage::with_upload_concurrency(NonZeroUsize)` to raise that limit deliberately;
buffering grows with part size and concurrency. This replaces the native Lance
adapter's previous implicit concurrency of eight. Clones carry both settings.
Parquet and Lance sessions can override them using `WriteOptions.upload.part_size`
and `WriteOptions.upload.concurrency`; unset fields preserve the Storage settings.

`Storage::object_store()` is always available for native SDKs. The adapter delegates
ordinary object operations to `OpendalStore`, while multipart uploads use the same
storage upload policy as `Storage::writer()`.

## HTTP downloads

A `DownloadPool` sends HTTP(S) downloads through a `Storage`, so they use the
same retry, timeout, request-budget, and transport configuration as every other
storage. Build it with `DownloadPool::http_storage(config)`, adjust it with
`with_http_concurrency_limit` or `with_http_transport`, and install it with
`with_http_storage`. Each request goes to the complete source URL, including
signed queries.

```rust,ignore
use open_arrow_sink_storage::{DownloadPool, HttpConcurrencyLimitLayer, StorageConfig};

let shared_limit = HttpConcurrencyLimitLayer::new(32)?;
let http = DownloadPool::http_storage(StorageConfig::default())?
    .with_http_concurrency_limit(shared_limit.clone())
    .with_http_transport(transport);
let pool = DownloadPool::with_defaults()?.with_http_storage(http)?;
```

`Storage::with_http_transport` replaces the provider transport for shared
authentication, instrumentation, or in-memory test fixtures without installing a
process-global default. The current request budget and operator layers still apply.
Returned chunks retain the same buffer accounting.

## Shared source configuration

Build an `Arc<StorageResolver>` with `StorageResolver::with_storage`, then pass
clones to `DownloadPool::with_source_resolver` and
`LanceDatasetSink::with_source_resolver`. This shares explicit source operators,
credentials, and the URL-derived cache across downloads and native Lance blob
reads. See the [Lance example](../sink/lance/README.md#independent-blob-sources).
`with_source_resolver` replaces prior registrations; `with_source_storage`
subsequently adds a store only for that consumer. HTTP(S) downloads still use
`with_http_storage` independently.

## Related documentation

[Usage](../docs/usage.md) · [Architecture](../docs/architecture.md) ·
[Support matrix](../docs/support.md) · [Write layers](../layer/README.md) ·
[Extend a sink](../docs/extending-sinks.md)
