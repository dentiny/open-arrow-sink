# Sinks

A sink maps Arrow batches to a typed destination and owns encoding, publication,
and cleanup. Start with [usage](../docs/usage.md) and the
[capability matrix](../docs/support.md), then choose a backend:

| Sink | Destination | Supported modes | Guide |
| --- | --- | --- | --- |
| `ParquetDirectorySink` | Local/cloud URL directory | Create, Append, filesystem staged Overwrite | [Parquet](parquet/README.md) |
| `IcebergTableSink` | Existing catalog table | Append; existing partition specs supported | [Iceberg](iceberg/README.md) |
| `LanceDatasetSink` | Local/cloud dataset URL | Create, Append, versioned Overwrite | [Lance](lance/README.md) |

All three support the default `Ordering::Unordered` and implement `ParallelSink`.
Iceberg additionally requires `WriteMode::Append`; the shared default Create is
unsupported. [Write layers](../layer/README.md) add reusable policy and execution.
To add another destination, follow [extend a sink](../docs/extending-sinks.md).
