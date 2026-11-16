//! Translate shared write options into Lance schema metadata and native parameters.
//! This module performs no destination I/O and owns the configuration conversion order.
use std::sync::Arc;

use arrow_schema::Schema;
use lance::{
    blob::{BlobFieldOptions, blob_field_with_options},
    dataset::{ExternalBlobMode as LanceBlobMode, WriteMode as LanceWriteMode, WriteParams},
};
use open_arrow_sink_core::{
    BlobHandleConfig, ExternalBlobMode, Result, SchemaRef, WriteMode, WriteOptions,
};

/// Apply compression before preparing blob descriptors, then attach layout options.
/// Both ordinary and blob-handle writers use this encoding schema pipeline.
pub(crate) fn writer_schema(schema: &SchemaRef, options: &WriteOptions) -> Result<SchemaRef> {
    let compressed = crate::compression::apply_compression(schema, &options.compression);
    let prepared = crate::blob::storage_schema(&compressed)?;
    Ok(blob_schema(&prepared, options.blob_handle.as_ref()))
}

pub(crate) fn write_params(options: &WriteOptions) -> Result<WriteParams> {
    let defaults = WriteParams::default();
    let default_blob = BlobHandleConfig::default();
    let blob = options.blob_handle.as_ref().unwrap_or(&default_blob);
    Ok(WriteParams {
        external_blob_mode: match blob.external_blob_mode {
            ExternalBlobMode::Reference => LanceBlobMode::Reference,
            ExternalBlobMode::Ingest => LanceBlobMode::Ingest,
        },
        data_storage_version: crate::validation::parse_storage_version(
            options.storage_version.as_deref(),
        )?,
        mode: match options.mode {
            WriteMode::Create => LanceWriteMode::Create,
            WriteMode::Append => LanceWriteMode::Append,
            WriteMode::Overwrite => LanceWriteMode::Overwrite,
        },
        max_bytes_per_file: options
            .target_file_size
            .unwrap_or(defaults.max_bytes_per_file),
        ..defaults
    })
}

/// Apply per-write layout options to top-level native logical or prepared blob fields.
/// Keep the input shape and metadata, overriding only explicitly configured thresholds.
fn blob_schema(schema: &SchemaRef, config: Option<&BlobHandleConfig>) -> SchemaRef {
    let Some(config) = config else {
        return schema.clone();
    };
    if config.inline_size_threshold.is_none() && config.dedicated_size_threshold.is_none() {
        return schema.clone();
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            if field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str)
                != Some("lance.blob.v2")
            {
                return field.clone();
            }
            let native = blob_field_with_options(
                field.name(),
                field.is_nullable(),
                BlobFieldOptions {
                    inline_size_threshold: config.inline_size_threshold,
                    dedicated_size_threshold: config.dedicated_size_threshold,
                },
            );
            let mut metadata = field.metadata().clone();
            metadata.extend(native.metadata().clone());
            Arc::new(field.as_ref().clone().with_metadata(metadata))
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}
