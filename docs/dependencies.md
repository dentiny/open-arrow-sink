# Dependency compatibility

All workspace crates expose Arrow 59 types. Update Arrow, Parquet, DataFusion
and the backend libraries together; mixing Arrow major versions produces
incompatible batches and schemas.

| Dependency | Requirement or pinned revision |
| --- | --- |
| Arrow / Parquet | `59.2.0` |
| DataFusion | `55.0.0` |
| Lance crate family | `fc8f59e53233a46f0d73604d66254142802389c9` (15.1.0-beta.2) |
| Iceberg | `b879b9626754501f025516ce0f0e9535fa6fd414` |
| OpenDAL | `0.59.5` |
| object_store | `0.14.2` |
| object_store_opendal | `0.60.5` |

Lance is pinned to the upstream main revision checked for this upgrade. Every
direct Lance dependency uses the same repository URL and full revision so Cargo
also resolves its internal crates from that checkout. Iceberg is pinned to the
upstream revision immediately before its Arrow 60 upgrade; its published 0.11
release still uses Arrow 58. Its Git package reports version 0.10.1.

Cargo's Arrow/Parquet requirements allow compatible 59.x updates and DataFusion
allows compatible 55.x updates. `Cargo.lock` records the exact versions tested.
Rust 1.98 is the workspace minimum required by the Lance revision, and the
toolchain file selects 1.98.1.

Run workspace Clippy and tests with all features when changing these pins.
