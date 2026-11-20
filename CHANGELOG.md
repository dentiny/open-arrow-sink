# Changelog

## 0.6.3 - 2026-11-19

- Share one prepared Lance dataset across fragment writers to avoid repeated dataset preparation.
- Release all 7 public workspace crates at 0.6.3 with matching internal dependency requirements.

## 0.6.2 - 2026-11-19

- Allow Lance overwrite to recover destinations containing uncommitted files without a manifest. Preserve Create/Append validation and propagate other dataset errors.
- Release all 7 public workspace crates at 0.6.2 with matching internal dependency requirements.

## 0.6.1 - 2026-11-19

- Support skipping missing Lance source blobs with explicit failure reporting and download metrics.
- Release all 7 public workspace crates at 0.6.1 with matching internal dependency requirements.

## 0.6.0 - 2026-11-18

- Configure a global per-read `io_request_size` for blob network reads.
- Drive accepted parallel writes in writer-owned background tasks, preserving at most one accepted batch per physical writer; nonempty parallel writes now require the application's Tokio runtime.
- Forward unordered Lance blob downloads in completion order while retaining bounded in-flight downloads.
- Reuse single-row blob payload buffers and avoid an intermediate network buffer copy.
- Release all 7 public workspace crates at 0.6.0 with matching internal dependency requirements.

## 0.5.1 - 2026-11-18

- Replace UnorderedWriteLayer with ParallelIngestionLayer for fixed or metadata-driven writer concurrency.
- Expose input size hints and ingestion plans, coordinating automatic writer counts with target file sizes.
- Release all 7 public workspace crates at 0.5.1 with matching internal dependency requirements.

## 0.5.0 - 2026-11-18

- Handle media sources and inline data URLs in the default storage downloader.
- Add DownloadContext::with_source_storage so credential adapters can reuse the default downloader's HTTP dispatch, chunking, metrics, and request budget.
- Add workspace Rust and TOML lint targets.
- Release all seven public workspace crates at 0.5.0 with matching internal dependency requirements.

## 0.4.0 - 2026-11-17

- Remove blob download byte limits.
- Honor HTTP `Retry-After` using released OpenDAL APIs, removing the Git dependency patches.
- Release all seven public workspace crates at 0.4.0 with matching internal dependency requirements.

## 0.3.0 - 2026-11-17

- Add `SinkExt::write_all` to consume complete batch streams, commit at EOF, and abort on input or write errors.
- Return `CommitSummary` from writers, session committers, and `write_all`, including actual committed rows and failed blob row counts. Custom implementations must return or forward the summary.
- Support nested URL fields in `DownloadBlobLayer`.
- Add `Storage::for_url` to bind an existing OpenDAL operator to a target URL.
- Preserve missing-object error classification and classify invalid Lance compression paths as configuration errors.
- Simplify destination resolution, operation observation, blob materialization, and backend writer internals; expand shared conformance and round-trip test coverage.
- Release all seven public workspace crates at 0.3.0 with matching internal dependency requirements.

## 0.2.0 - 2026-11-16

- Download S3 blobs whose URLs include MD5 metadata query parameters.
- Replace positional return tuples with named result structs.
- Release all public workspace crates at 0.2.0 with matching internal dependency requirements; retain Arrow 58, Lance 14.0.3, and OpenDAL 0.59.5 compatibility.

## 0.1.1

- Update the Lance crate family to 14.0.3.
- Require OpenDAL 0.59.5 and object_store_opendal 0.60.5 for shared storage.
- Release the workspace together with matching internal dependency requirements.

## 0.1.0

Initial release of the Arrow RecordBatch sink workspace:

- Shared sink, writer, session, and commit contracts.
- OpenDAL storage with optional AWS, GCP, and Azure support.
- Composable write layers for concurrency, metrics, schema adaptation, and blobs.
- Parquet directory, Iceberg append, and Lance dataset backends.
- A facade crate with opt-in backend, cloud, and Parquet codec features.
