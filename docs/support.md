# Backend support matrix

All bundled sinks implement `ParallelSink` and require `Ordering::Unordered`,
even with one writer. `WriteOptions::default()` requests `Create` and
`PreserveInput`; set ordering explicitly and use `Append` for Iceberg.
Inspect `sink.capability()` for the configured sink, then apply the request-time
restrictions below. Cargo features are additive.

| Capability | Parquet directory | Iceberg table | Lance dataset |
| --- | --- | --- | --- |
| Facade feature | `sink-parquet` | `sink-iceberg` | `sink-lance` |
| Request | `Url` | SDK `TableIdent` | `Url` |
| Create | New directory/prefix | Unsupported | New dataset |
| Append | Existing directory; validate root-level Parquet schemas | Existing writable table | Existing dataset with matching effective schema |
| Overwrite | Unsupported | Unsupported | New manifest version; historical versions retained |
| Preserve input order | Unsupported | Unsupported | Unsupported |
| Partition routing | Unsupported | Existing spec: Identity, Year, Month, Day, Hour, Bucket, Truncate, Void | Unsupported |
| Compression, global | Uncompressed; opt-in Snappy, Gzip, Zstd, Lz4Raw | Uncompressed, Snappy, Gzip, Zstd, Lz4Raw | Uncompressed, Zstd, Lz4Raw |
| Compression, per-column | Same enabled codecs as global | Unsupported | Uncompressed, Zstd, Lz4Raw |
| Structural encoding | Unsupported | Unsupported | Miniblock, Fullzip; global and per-column |
| Explicit Zstd level | 1–22 | 1–22 | 0–22 |
| Session upload part size / concurrency | Supported | Unsupported; configure catalog Storage | Supported |
| Target file size | Soft target; rolls between batches, default one file per writer | SDK soft target; table default otherwise | Native soft target; SDK default otherwise |
| Index requests | Unsupported | Unsupported | Scalar/BTree, Vector/IVF_FLAT L2, Text/Inverted, BloomFilter |
| URL columns via `DownloadBlobLayer` | Unsupported | Unsupported | Explicit top-level text columns; single and parallel writers |
| Native streaming blobs | Unsupported | Unsupported | Unsupported; use logical blob inputs |
| Explicit storage version | Unsupported | Unsupported | `2.2` or `2.3` |
| Empty successful session | One readable file per opened writer | No new snapshot | Readable empty dataset on Create; see native mode semantics |

Partition options select the transform category already present in the Iceberg
spec; every field must use that category. They do not define a new spec. Mixed
transform categories are unsupported. Schema metadata must retain Iceberg field IDs.
Lance append compares the effective schema, including compression metadata.
Lance maps `Lz4Raw` to its native `lz4` policy; some Fullzip per-value paths in
Lance 14 may substitute Zstd. Miniblock encoding uses LZ4.

## Features and storage

| Need | Facade feature | Direct component feature |
| --- | --- | --- |
| Parquet S3 / GCS / Azure | `parquet-aws` / `parquet-gcp` / `parquet-azure` | `aws` / `gcp` / `azure` on Parquet |
| Parquet page codecs | `parquet-snappy`, `parquet-gzip`, `parquet-zstd`, `parquet-lz4` | `snappy`, `gzip`, `zstd`, `lz4` on Parquet |
| Lance S3 / GCS / Azure | `lance-aws` / `lance-gcp` / `lance-azure` | `aws` / `gcp` / `azure` on Lance |
| Iceberg storage | `sink-iceberg`; configure catalog FileIO or `OpenDalStorageFactory` | Catalog / configured OpenDAL operator |
| Shared contracts, write layers, storage | Always exported | Core, layer, storage crates |

Cloud/codec facade features also enable their sink. `sink-parquet` alone writes
uncompressed files. Compression here is output-format compression, not an outer
`.gz` wrapper. Filesystem URL resolution is built in; for shared in-memory data,
provide one configured `Storage` rather than resolving fresh `memory://` stores.

## Visibility and failure

| Backend | Before commit | Successful commit | Failure or abort |
| --- | --- | --- | --- |
| Parquet | Parts may already be visible | Finish footers | Output retained; no general directory transaction/fsync guarantee |
| Iceberg | Unreferenced data files may exist | One fast-append catalog transaction | Output retained; commit outcome can be uncertain |
| Lance | Uncommitted fragments/blobs may exist | Data manifest, followed by optional independent index commits | Existing data is preserved; publication errors can be uncertain; index errors leave committed data and successful indexes |

Failures return directly without automatic abort, deletion, or rollback.
Explicit abort stops writing and retains persisted output. Drop performs no async cleanup. Applications must exclude
unsupported concurrent destination mutation and reconcile uncertain commits
before retrying. No general upsert, automatic schema evolution, sorting, or
session-level retry is supplied.

For reconciliation, manual cleanup, and subsequent Overwrite behavior, see
[recover from a failed commit](usage.md#recover-from-a-failed-commit).

Details: [Parquet](../sink/parquet/README.md),
[Iceberg](../sink/iceberg/README.md), [Lance](../sink/lance/README.md).
