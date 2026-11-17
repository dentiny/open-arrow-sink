# Changelog

## Unreleased

- Return `CommitSummary` from writer/session commits and stream-writing helpers,
  with actual committed rows, session-local blob recovery counts, and Lance version.
  This changes custom writer/committer implementations and replaces the stream
  helper's accepted-input `WriteSummary`.
- Use the shared `Error` and `Result` for `write_all`, with input errors converted
  at the stream boundary; remove the separate `WriteAllError` wrapper.
- Count actual committed output in `MetricsLayer`, including blob row skipping.

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
