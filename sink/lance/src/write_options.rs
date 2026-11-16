//! Translate shared write options into Lance schema metadata and native parameters.
//! This module performs no destination I/O and owns the configuration conversion order.
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use lance::{
    blob::{BlobDescriptorArrayBuilder, BlobFieldOptions, blob_field, blob_field_with_options},
    dataset::{ExternalBlobMode as LanceBlobMode, WriteMode as LanceWriteMode, WriteParams},
};
use open_arrow_sink_core::{
    BlobHandleConfig, ExternalBlobMode, Result, SchemaRef, WriteMode, WriteOptions, is_blob_field,
};

/// The caller's batch shape, Lance's persisted shape, and the native writer's shape.
/// Derive both native shapes together so preparation never decodes an encoded schema.
#[derive(Clone)]
pub(crate) struct LanceSchemas {
    pub input: SchemaRef,
    pub logical: SchemaRef,
    pub encoded: SchemaRef,
}

impl LanceSchemas {
    pub fn new(input: SchemaRef, options: &WriteOptions) -> Result<Self> {
        crate::validation::validate_blob_schema(&input)?;
        let compressed = crate::compression::apply_compression(&input, &options.compression);
        let mut logical_fields = Vec::with_capacity(compressed.fields().len());
        let mut encoded_fields = Vec::with_capacity(compressed.fields().len());
        let mut has_blobs = false;
        for field in compressed.fields() {
            if is_blob_field(field) {
                has_blobs = true;
                let descriptor = BlobDescriptorArrayBuilder::new(field.name())
                    .field()
                    .clone();
                let mut metadata = field.metadata().clone();
                metadata.extend(descriptor.metadata().clone());
                let common = blob_field_options(
                    &Arc::new(field.as_ref().clone().with_metadata(metadata)),
                    options.blob_handle.as_ref(),
                );
                let metadata = common.metadata().clone();
                logical_fields.push(Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(
                            blob_field(field.name(), field.is_nullable())
                                .data_type()
                                .clone(),
                        )
                        .with_metadata(metadata.clone()),
                ));
                encoded_fields.push(Arc::new(
                    descriptor
                        .with_nullable(field.is_nullable())
                        .with_metadata(metadata),
                ));
            } else {
                let field = blob_field_options(field, options.blob_handle.as_ref());
                logical_fields.push(field.clone());
                encoded_fields.push(field);
            }
        }
        let mut metadata = compressed.metadata().clone();
        if has_blobs {
            metadata.insert("lance.blob.managed".into(), "true".into());
        }
        let encoded = Arc::new(Schema::new_with_metadata(encoded_fields, metadata.clone()));
        let logical = if has_blobs {
            Arc::new(Schema::new_with_metadata(logical_fields, metadata))
        } else {
            encoded.clone()
        };
        Ok(Self {
            input,
            logical,
            encoded,
        })
    }
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
fn blob_field_options(field: &Arc<Field>, config: Option<&BlobHandleConfig>) -> Arc<Field> {
    let Some(config) = config else {
        return field.clone();
    };
    if (config.inline_size_threshold.is_none() && config.dedicated_size_threshold.is_none())
        || field
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
}
