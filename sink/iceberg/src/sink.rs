use iceberg::table::Table;
use std::{num::NonZeroUsize, sync::Arc};

use iceberg::spec::{PartitionKey, Struct, TableProperties};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableIdent};
use open_arrow_sink_core::{
    Compression, CompressionCapability, CompressionCodec, DataWriter, Ordering, ParallelSink,
    PartitionType, Result, SchemaRef, Sink, SinkCapability, WriteOptions, WriteSession, Writer,
    async_trait,
};

use crate::{
    committer::IcebergCommitter,
    error::backend,
    partition::PartitionWriterAdapter,
    validation::{validate_options, validate_table},
    writer::IcebergTableWriter,
};

/// Appends to an existing table in a caller-configured catalog.
/// Append is the only advertised write mode and must be requested explicitly.
/// The shared default Create and explicit Overwrite are rejected before catalog access: iceberg-rust 0.11 exposes
/// no public full-overwrite transaction, and this request carries no table
/// creation configuration. Tables are never dropped/recreated to emulate overwrite.
pub struct IcebergTableSink {
    catalog: Arc<dyn Catalog>,
}

impl IcebergTableSink {
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl Sink for IcebergTableSink {
    type Request = TableIdent;

    fn capability(&self) -> SinkCapability {
        SinkCapability {
            supported_modes: &[open_arrow_sink_core::WriteMode::Append],
            ordering: Ordering::Unordered,
            compression: CompressionCapability {
                global: &[
                    CompressionCodec::Uncompressed,
                    CompressionCodec::Snappy,
                    CompressionCodec::Gzip,
                    CompressionCodec::Zstd,
                    CompressionCodec::Lz4Raw,
                ],
                per_column: &[],
            },
            supported_partitions: vec![
                PartitionType::Identity,
                PartitionType::Year,
                PartitionType::Month,
                PartitionType::Day,
                PartitionType::Hour,
                PartitionType::Bucket,
                PartitionType::Truncate,
                PartitionType::Void,
            ],
            supported_indexes: &[],
        }
    }

    async fn open(
        &self,
        request: TableIdent,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validate_options(&schema, &options, self.capability())?;
        let table = self.catalog.load_table(&request).await.map_err(backend)?;
        validate_table(&table, &schema, options.partition)?;
        self.create_writer(table, schema, options)
            .await
            .map(|w| w as Box<dyn Writer>)
    }
}

#[async_trait]
impl ParallelSink for IcebergTableSink {
    type Output = (u64, Vec<iceberg::spec::DataFile>);
    async fn open_writers(
        &self,
        request: TableIdent,
        schema: SchemaRef,
        options: WriteOptions,
        count: NonZeroUsize,
    ) -> Result<WriteSession<Self::Output>> {
        validate_options(&schema, &options, self.capability())?;
        let table = self.catalog.load_table(&request).await.map_err(backend)?;
        validate_table(&table, &schema, options.partition)?;
        let mut writers: Vec<Box<dyn DataWriter<Output = Self::Output>>> = Vec::new();
        for _ in 0..count.get() {
            match self
                .create_writer(table.clone(), schema.clone(), options.clone())
                .await
            {
                Ok(writer) => writers.push(writer),
                Err(error) => {
                    for writer in writers {
                        let _ = writer.abort().await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(WriteSession {
            writers,
            committer: Box::new(IcebergCommitter {
                catalog: self.catalog.clone(),
                table,
            }),
        })
    }
}

impl IcebergTableSink {
    async fn create_writer(
        &self,
        table: Table,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<IcebergTableWriter>> {
        let metadata = table.metadata();
        // A private session prefix lets abort remove only this writer's files.
        let id = uuid::Uuid::new_v4().to_string();
        let prefix = format!(
            "{}/",
            DefaultLocationGenerator::new(metadata)
                .map_err(backend)?
                .generate_location(None, &format!("open-arrow-sink-{id}"))
        );
        let mut properties = metadata.properties().clone();
        if let Some(codec) = options.compression.global {
            let value = match codec {
                Compression::Uncompressed => "uncompressed",
                Compression::Snappy => "snappy",
                Compression::Gzip => "gzip",
                Compression::Zstd { .. } => "zstd",
                Compression::Lz4Raw => "lz4_raw",
            };
            properties.insert(
                TableProperties::PROPERTY_PARQUET_COMPRESSION_CODEC.into(),
                value.into(),
            );
            // An explicit codec replaces the table's old codec and level.
            properties.remove(TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL);
            if let Compression::Zstd { level: Some(level) } = codec {
                properties.insert(
                    TableProperties::PROPERTY_PARQUET_COMPRESSION_LEVEL.into(),
                    level.to_string(),
                );
            }
        }
        let parquet = ParquetWriterBuilder::from_table_properties(
            &TableProperties::new(&properties),
            metadata.current_schema().clone(),
        )
        .map_err(backend)?;
        let rolling = RollingFileWriterBuilder::new(
            parquet,
            options
                .target_file_size
                .unwrap_or(TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT),
            table.file_io().clone(),
            DefaultLocationGenerator::with_data_location(prefix.trim_end_matches('/').to_owned()),
            DefaultFileNameGenerator::new(id, None, iceberg::spec::DataFileFormat::Parquet),
        );
        let builder = DataFileWriterBuilder::new(rolling);
        let inner: Box<dyn IcebergWriter> = if options.partition.is_some()
            && !metadata.default_partition_spec().is_unpartitioned()
        {
            Box::new(
                PartitionWriterAdapter::try_new(
                    builder,
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().clone(),
                )
                .map_err(backend)?,
            )
        } else {
            let partition = PartitionKey::new(
                metadata.default_partition_spec().as_ref().clone(),
                metadata.current_schema().clone(),
                // All-void specs are unpartitioned to the SDK, but their file
                // metadata must retain one null value per partition field.
                Struct::from_iter(
                    metadata
                        .default_partition_spec()
                        .fields()
                        .iter()
                        .map(|_| None),
                ),
            );
            Box::new(builder.build(Some(partition)).await.map_err(backend)?)
        };
        Ok(Box::new(IcebergTableWriter::new(
            self.catalog.clone(),
            table,
            schema,
            prefix,
            inner,
            options,
        )))
    }
}
