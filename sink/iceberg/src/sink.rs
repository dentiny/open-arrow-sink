use std::{num::NonZeroUsize, sync::Arc};

use iceberg::spec::TableProperties;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableIdent};
use open_arrow_sink_core::{
    CompressionCapability, CompressionCodec, DataWriter, Error, Ordering, ParallelSink,
    PartitionType, Result, SchemaRef, Sink, SinkCapability, WriteOptions, WriteSession, Writer,
    async_trait,
};

use crate::{
    committer::IcebergCommitter,
    compression::apply_compression,
    partition::{PartitionWriterAdapter, WriterKind},
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
        SinkCapability::default()
            .with_supported_modes(&[open_arrow_sink_core::WriteMode::Append])
            .with_ordering(Ordering::Unordered)
            .with_compression(
                CompressionCapability::default()
                    .with_global(&[
                        CompressionCodec::Uncompressed,
                        CompressionCodec::Snappy,
                        CompressionCodec::Gzip,
                        CompressionCodec::Zstd,
                        CompressionCodec::Lz4Raw,
                    ])
                    .with_per_column(&[])
                    .with_structural_encodings(&[])
                    .with_nested_columns(false),
            )
            .with_supported_partitions(vec![
                PartitionType::Identity,
                PartitionType::Year,
                PartitionType::Month,
                PartitionType::Day,
                PartitionType::Hour,
                PartitionType::Bucket,
                PartitionType::Truncate,
                PartitionType::Void,
            ])
            .with_supports_blob_handle(false)
            .with_supports_storage_version(false)
            .with_supported_indexes(&[])
    }

    async fn open(
        &self,
        request: TableIdent,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        self.open_writer(request, schema, options).await
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
        let table = self
            .catalog
            .load_table(&request)
            .await
            .map_err(Error::backend)?;
        validate_table(&table, &schema, options.partition)?;
        let metadata = table.metadata();
        let kind = WriterKind::new(
            metadata.default_partition_spec().clone(),
            metadata.current_schema().clone(),
        );
        let mut properties = metadata.properties().clone();
        apply_compression(&mut properties, options.compression.global);
        let properties = TableProperties::new(&properties);
        let parquet = ParquetWriterBuilder::from_table_properties(
            &properties,
            metadata.current_schema().clone(),
        )
        .map_err(Error::backend)?;
        let target_file_size = match options.target_file_size {
            Some(size) => size,
            None => properties
                .write_target_file_size_bytes()
                .map_err(Error::backend)?,
        };
        let location = DefaultLocationGenerator::new(metadata).map_err(Error::backend)?;
        let mut writers: Vec<Box<dyn DataWriter<Output = Self::Output>>> = Vec::new();
        for _ in 0..count.get() {
            let rolling = RollingFileWriterBuilder::new(
                parquet.clone(),
                target_file_size,
                table.file_io().clone(),
                location.clone(),
                DefaultFileNameGenerator::new(
                    uuid::Uuid::new_v4().to_string(),
                    None,
                    iceberg::spec::DataFileFormat::Parquet,
                ),
            );
            let builder = DataFileWriterBuilder::new(rolling);
            let inner: Box<dyn IcebergWriter> = match &kind {
                WriterKind::Fanout(spec) => Box::new(
                    PartitionWriterAdapter::try_new(
                        builder,
                        metadata.current_schema().clone(),
                        spec.clone(),
                    )
                    .map_err(Error::backend)?,
                ),
                WriterKind::Fixed(key) => Box::new(
                    builder
                        .build(Some(key.clone()))
                        .await
                        .map_err(Error::backend)?,
                ),
            };
            writers.push(Box::new(IcebergTableWriter::new(schema.clone(), inner)));
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
