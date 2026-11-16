# Changelog

## 0.1.0

Initial release of the Arrow RecordBatch sink workspace:

- Shared sink, writer, session, and commit contracts.
- OpenDAL storage with optional AWS, GCP, and Azure support.
- Composable write layers for concurrency, metrics, schema adaptation, and blobs.
- Parquet directory, Iceberg append, and Lance dataset backends.
- A facade crate with opt-in backend, cloud, and Parquet codec features.
