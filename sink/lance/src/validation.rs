//! Configuration, destination, and blob validation.
use crate::config::SessionOptions;
use arrow_schema::{DataType, Schema};
use open_arrow_sink_core::{
    Compression, CompressionOptions, Result, SinkCapability, ZSTD_COMPRESSION_LEVEL_RANGE,
    validation,
};

use crate::error;

pub(crate) fn validate_options(
    schema: &Schema,
    options: &SessionOptions,
    capability: SinkCapability,
) -> Result<()> {
    validation::validate_options(schema, options, capability)?;
    if let Some(version) = options.storage_version {
        if !matches!(
            version,
            lance_file::version::LanceFileVersion::V2_2
                | lance_file::version::LanceFileVersion::V2_3
        ) {
            return Err(error::unsupported_storage_version(version));
        }
    }
    validate_encoding(schema, &options.compression)?;
    validate_compression(&options.compression)?;
    crate::index::validate_indexes(schema, &options.indexes)
}

fn validate_compression(options: &CompressionOptions) -> Result<()> {
    for compression in options
        .global
        .iter()
        .chain(options.columns.values())
        .chain(options.nested_columns.values())
    {
        // Lance accepts 0 as the native Zstd default, in addition to positive levels.
        if let Compression::Zstd { level: Some(level) } = compression
            && *level != 0
            && !ZSTD_COMPRESSION_LEVEL_RANGE.contains(level)
        {
            return Err(error::invalid_compression_level(*level));
        }
    }
    Ok(())
}

fn validate_encoding(schema: &Schema, options: &CompressionOptions) -> Result<()> {
    // Native blob descriptors are an opaque Lance representation. Generic Arrow
    // path resolution in core must not know about Lance extension types.
    for path in options.nested_columns.keys() {
        if (1..path.len()).any(|end| {
            validation::nested_field(schema, &path[..end]).is_some_and(|field| {
                field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str)
                    == Some("lance.blob.v2")
            })
        }) {
            return Err(open_arrow_sink_core::Error::Unsupported(format!(
                "nested compression inside native blob column: {path:?}"
            )));
        }
    }
    for field in schema.fields() {
        let encoding = options
            .column_encodings
            .get(field.name())
            .copied()
            .or(options.structural_encoding);
        if encoding.is_some()
            && (field
                .metadata()
                .get(open_arrow_sink_core::BLOB_METADATA_KEY)
                .map(String::as_str)
                == Some("true")
                || field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str)
                    == Some("lance.blob.v2"))
        {
            return Err(open_arrow_sink_core::Error::Unsupported(format!(
                "structural encoding on blob column {}",
                field.name()
            )));
        }
    }
    Ok(())
}

pub(crate) async fn validate_existing_dataset(
    builder: lance::dataset::builder::DatasetBuilder,
    schema: Option<&Schema>,
) -> Result<()> {
    let dataset = builder.load().await.map_err(error::backend)?;
    if !matches!(
        dataset.manifest().data_storage_format.version,
        lance_file::version::ConcreteFileVersion::V2_2
            | lance_file::version::ConcreteFileVersion::V2_3
    ) {
        return Err(error::unsupported_storage_version(
            dataset.manifest().data_storage_format.version,
        ));
    }
    if let Some(schema) = schema
        && &Schema::from(dataset.schema()) != schema
    {
        return Err(error::schema_mismatch());
    }
    Ok(())
}

pub(crate) fn validate_write_state(failed: bool) -> Result<()> {
    if failed {
        return Err(error::failed());
    }
    Ok(())
}

pub(crate) fn validate_blob_schema(schema: &Schema) -> Result<()> {
    for field in schema.fields() {
        if field
            .metadata()
            .get(open_arrow_sink_core::BLOB_METADATA_KEY)
            .map(String::as_str)
            == Some("true")
            && field.data_type() != &DataType::LargeBinary
        {
            return Err(error::invalid_blob_column(field.name()));
        }
    }
    Ok(())
}
pub(crate) fn validate_parallel_blob_schema(schema: &Schema) -> Result<()> {
    validate_blob_schema(schema)?;
    if schema.fields().iter().any(|field| {
        field
            .metadata()
            .get(open_arrow_sink_core::BLOB_METADATA_KEY)
            .map(String::as_str)
            == Some("true")
    }) {
        return Err(error::parallel_blobs());
    }
    Ok(())
}
pub(crate) fn validate_blob_state(failed: bool, session: &crate::blob::BlobSession) -> Result<()> {
    if failed || session.failed.load(std::sync::atomic::Ordering::Acquire) {
        return Err(error::blob_failed());
    }
    Ok(())
}
pub(crate) fn validate_blob_commit(failed: bool, session: &crate::blob::BlobSession) -> Result<()> {
    if session.active.load(std::sync::atomic::Ordering::Acquire) != 0
        || validate_blob_state(failed, session).is_err()
    {
        return Err(error::unfinished_blobs());
    }
    Ok(())
}
pub(crate) fn validate_blob_placements(
    batch: &open_arrow_sink_core::RecordBatch,
    blobs: Vec<open_arrow_sink_core::BlobPlacement>,
    session: &std::sync::Arc<crate::blob::BlobSession>,
) -> Result<std::collections::HashMap<(usize, usize), open_arrow_sink_core::BlobHandle>> {
    use crate::blob::NativeBlob;
    use arrow_array::{Array, LargeBinaryArray};
    use open_arrow_sink_core::BLOB_METADATA_KEY;
    let mut placements = std::collections::HashMap::new();
    for blob in blobs {
        let field = batch
            .schema_ref()
            .fields()
            .get(blob.column)
            .ok_or_else(error::blob_column_out_of_bounds)?;
        if blob.row >= batch.num_rows()
            || field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) != Some("true")
        {
            return Err(error::blob_cell());
        }
        let array = batch
            .column(blob.column)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| error::invalid_blob_column(field.name()))?;
        if array.is_null(blob.row) || !array.value(blob.row).is_empty() {
            return Err(error::blob_placeholder());
        }
        let native = blob
            .blob
            .downcast_ref::<NativeBlob>()
            .ok_or_else(error::foreign_blob)?;
        if !std::sync::Arc::ptr_eq(&native.session, session) {
            return Err(error::blob_owner());
        }
        if placements
            .insert((blob.column, blob.row), blob.blob)
            .is_some()
        {
            return Err(error::duplicate_blob_placement());
        }
    }
    Ok(placements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn nested_compression_cannot_address_native_blob_internals() {
        let blob = Field::new(
            "blob",
            DataType::Struct(
                vec![Arc::new(Field::new("data", DataType::LargeBinary, false))].into(),
            ),
            false,
        )
        .with_metadata([("ARROW:extension:name".into(), "lance.blob.v2".into())].into());
        for (field, path) in [
            (blob.clone(), vec!["blob".into(), "data".into()]),
            (
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(vec![Arc::new(blob)].into()),
                        false,
                    ))),
                    false,
                ),
                vec!["items".into(), "blob".into(), "data".into()],
            ),
        ] {
            let schema = Schema::new(vec![field]);
            assert!(validation::nested_field(&schema, &path).is_some());
            let options = CompressionOptions {
                nested_columns: [(path, Compression::Zstd { level: None })].into(),
                ..Default::default()
            };
            assert!(validate_encoding(&schema, &options).is_err());
        }
    }
}
