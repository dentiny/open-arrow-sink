//! Translate shared write options into Lance schema metadata and native parameters.
//! This module performs no destination I/O and owns the configuration conversion order.
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use lance::{
    blob::{BlobFieldOptions, blob_field_with_options},
    dataset::{ExternalBlobMode as LanceBlobMode, WriteMode as LanceWriteMode, WriteParams},
};
use open_arrow_sink_core::{
    BlobHandleConfig, ExternalBlobMode, Result, SchemaRef, WriteMode, WriteOptions, is_blob_field,
};

/// ```text
/// input schema:
///   payload: LargeBinary (nullable)
///     open-arrow-sink:blob = "true"
///
/// logical schema:
///   payload: Struct<data: LargeBinary, uri: Utf8> (nullable)
///     open-arrow-sink:blob = "true"
///     ARROW:extension:name = "lance.blob.v2"
/// ```
#[derive(Clone)]
pub(crate) struct LanceSchemas {
    /// Caller-provided batch schema.
    pub input: SchemaRef,
    /// Lance SDK input schema with blob types and per-write encoding metadata.
    pub logical: SchemaRef,
}

impl LanceSchemas {
    pub fn new(input: SchemaRef, options: &WriteOptions) -> Result<Self> {
        crate::validation::validate_blob_schema(&input)?;
        let compressed = crate::compression::apply_compression(&input, &options.compression)?;
        let mut has_marked_blobs = false;
        let fields = compressed
            .fields()
            .iter()
            .map(|field| {
                convert_blob_field(field, options.blob_handle.as_ref(), &mut has_marked_blobs)
            })
            .collect::<Vec<_>>();
        let mut metadata = compressed.metadata().clone();
        if has_marked_blobs {
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

/// Recursively convert blob-marked binary/URL fields to Lance's logical Blob type.
/// Apply only explicitly configured size thresholds to new and existing Blob fields,
/// preserving other metadata and nullability. Ordinary fields keep their types.
/// Set `has_marked_blobs` when a marked field is converted, without clearing its prior value;
/// the caller uses it to enable managed blobs in the schema metadata.
fn convert_blob_field(
    field: &Arc<Field>,
    config: Option<&BlobHandleConfig>,
    has_marked_blobs: &mut bool,
) -> Arc<Field> {
    let marked = is_blob_field(field);
    if marked || crate::blob::is_native_blob(field) {
        *has_marked_blobs |= marked;
        return logical_blob_field(field, config, marked);
    }
    let ty = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|child| convert_blob_field(child, config, has_marked_blobs))
                .collect(),
        ),
        DataType::List(item) => DataType::List(convert_blob_field(item, config, has_marked_blobs)),
        DataType::LargeList(item) => {
            DataType::LargeList(convert_blob_field(item, config, has_marked_blobs))
        }
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(convert_blob_field(item, config, has_marked_blobs), *size)
        }
        DataType::Map(item, sorted) => {
            DataType::Map(convert_blob_field(item, config, has_marked_blobs), *sorted)
        }
        _ => return field.clone(),
    };
    Arc::new(field.as_ref().clone().with_data_type(ty))
}

/// Marked bytes/URLs acquire Lance's logical struct type. Existing native Blob
/// fields retain their shape (including optional range fields); both receive only
/// explicitly requested threshold overrides, preserving other metadata.
fn logical_blob_field(
    field: &Field,
    config: Option<&BlobHandleConfig>,
    marked: bool,
) -> Arc<Field> {
    let native = blob_field_with_options(
        field.name(),
        field.is_nullable(),
        BlobFieldOptions {
            inline_size_threshold: config.and_then(|config| config.inline_size_threshold),
            dedicated_size_threshold: config.and_then(|config| config.dedicated_size_threshold),
        },
    );
    let mut metadata = field.metadata().clone();
    metadata.extend(native.metadata().clone());
    let ty = if marked {
        native.data_type()
    } else {
        field.data_type()
    };
    Arc::new(
        field
            .clone()
            .with_data_type(ty.clone())
            .with_metadata(metadata),
    )
}
