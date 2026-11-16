//! Translate shared write options into Lance schema metadata and native parameters.
//! This module performs no destination I/O and owns the configuration conversion order.
use std::sync::Arc;

use arrow_schema::{Field, Schema};
use lance::{
    blob::{BlobFieldOptions, blob_field, blob_field_with_options},
    dataset::{ExternalBlobMode as LanceBlobMode, WriteMode as LanceWriteMode, WriteParams},
};
use open_arrow_sink_core::{
    BlobHandleConfig, ExternalBlobMode, Result, SchemaRef, WriteMode, WriteOptions, is_blob_field,
};

/// Caller input and logical SDK input, including per-write encoding metadata.
#[derive(Clone)]
pub(crate) struct LanceSchemas {
    pub input: SchemaRef,
    pub logical: SchemaRef,
}

impl LanceSchemas {
    pub fn new(input: SchemaRef, options: &WriteOptions) -> Result<Self> {
        crate::validation::validate_blob_schema(&input)?;
        let compressed = crate::compression::apply_compression(&input, &options.compression)?;
        let mut has_blobs = false;
        let fields = compressed
            .fields()
            .iter()
            .map(|field| {
                let logical = if is_blob_field(field) {
                    has_blobs = true;
                    let native = blob_field(field.name(), field.is_nullable());
                    let mut metadata = field.metadata().clone();
                    metadata.extend(native.metadata().clone());
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(native.data_type().clone())
                            .with_metadata(metadata),
                    )
                } else {
                    field.clone()
                };
                blob_field_options(&logical, options.blob_handle.as_ref())
            })
            .collect::<Vec<_>>();
        let mut metadata = compressed.metadata().clone();
        if has_blobs {
            metadata.insert("lance.blob.managed".into(), "true".into());
        }
        Ok(Self {
            input,
            logical: Arc::new(Schema::new_with_metadata(fields, metadata)),
        })
    }
}

pub(crate) fn write_params(
    options: &WriteOptions,
    storage_version: Option<lance_file::version::LanceFileVersion>,
) -> WriteParams {
    let defaults = WriteParams::default();
    let default_blob = BlobHandleConfig::default();
    let blob = options.blob_handle.as_ref().unwrap_or(&default_blob);
    WriteParams {
        external_blob_mode: match blob.external_blob_mode {
            ExternalBlobMode::Reference => LanceBlobMode::Reference,
            ExternalBlobMode::Ingest => LanceBlobMode::Ingest,
        },
        data_storage_version: storage_version,
        mode: match options.mode {
            WriteMode::Create => LanceWriteMode::Create,
            WriteMode::Append => LanceWriteMode::Append,
            WriteMode::Overwrite => LanceWriteMode::Overwrite,
        },
        max_bytes_per_file: options
            .target_file_size
            .unwrap_or(defaults.max_bytes_per_file),
        ..defaults
    }
}

/// Apply per-write layout options to top-level logical blob fields.
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
