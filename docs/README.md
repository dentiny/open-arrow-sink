# Documentation

Start with [your first write](../README.md#write-your-first-dataset), then follow
one of these paths. Examples use Rust APIs from this checkout; source code and
backend READMEs define the behavior available in this release.

| Goal | Read |
| --- | --- |
| Build an ingestion/export application | [Usage](usage.md), [support matrix](support.md) |
| Configure ordering, modes, compression, file sizes, indexes, or blobs | [Options](options.md) |
| Configure credentials, URL namespaces, retries, and uploads | [Shared storage](../storage/README.md) |
| Use write decorators | [Layers](../layer/README.md) |
| Understand contracts and ownership | [Architecture](architecture.md), [core](../core/README.md) |
| Implement a new destination | [Extend a sink](extending-sinks.md) |
| Implement a new decorator | [Extend a layer](extending-layers.md) |
| Build examples, run checks, prepare publication | [Development](development.md) |

Backend guides: [Parquet](../sink/parquet/README.md),
[Iceberg](../sink/iceberg/README.md), [Lance](../sink/lance/README.md).

The reading flow follows the companion
[open-arrow-access documentation](https://github.com/dentiny/open-arrow-access/tree/main/docs):
quick start → composition → usage → architecture → extension contracts. The write
APIs have their own lifecycles and do not depend on that project.
