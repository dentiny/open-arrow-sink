//! Mandatory shared contracts, separate from optional business validation layers.
use std::collections::HashSet;

use crate::{Error, IndexSpec, Ordering, RecordBatch, Result, SinkCapability, WriteOptions};
use arrow_schema::Schema;

/// Check declared capabilities before opening a destination or producing side effects.
pub fn validate_options(options: &WriteOptions, capability: SinkCapability) -> Result<()> {
    if !options.indexes.is_empty() && !capability.indexes {
        return Err(Error::Unsupported("index construction".into()));
    }
    if options.target_file_size == Some(0) {
        return Err(Error::Unsupported(
            "target_file_size must be greater than zero".into(),
        ));
    }
    if options.ordering == Ordering::PreserveInput && capability.ordering != Ordering::PreserveInput
    {
        return Err(Error::Unsupported("preserving input row order".into()));
    }
    Ok(())
}

/// Exact schema equality, including field order, nullability and metadata.
/// Casting, schema reconciliation and evolution require an explicit adapter.
pub fn validate_batch(schema: &Schema, batch: &RecordBatch) -> Result<()> {
    if schema != batch.schema().as_ref() {
        return Err(Error::SchemaMismatch);
    }
    Ok(())
}

/// Validate index column existence and uniqueness independently of the backend.
/// Concrete backends additionally validate their supported column types.
pub fn validate_indexes(schema: &Schema, indexes: &[IndexSpec]) -> Result<()> {
    let mut columns = HashSet::new();
    for index in indexes {
        if !columns.insert(&index.column) {
            return Err(Error::Unsupported(format!(
                "multiple indexes requested for column {}",
                index.column
            )));
        }
        schema
            .field_with_name(&index.column)
            .map_err(|_| Error::Unsupported(format!("index column not found: {}", index.column)))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IndexKind;
    use arrow_schema::{DataType, Field};

    #[test]
    fn rejects_index_requests_when_capability_is_disabled() {
        let capability = SinkCapability {
            ordering: Ordering::Unordered,
            indexes: false,
        };
        let mut options = WriteOptions {
            ordering: Ordering::Unordered,
            ..Default::default()
        };
        assert!(options.indexes.is_empty());
        assert!(validate_options(&options, capability).is_ok());
        for kind in [
            IndexKind::Scalar,
            IndexKind::Vector,
            IndexKind::Text,
            IndexKind::BloomFilter,
        ] {
            options.indexes = vec![IndexSpec::new("id", kind)];
            assert!(matches!(
                validate_options(&options, capability),
                Err(Error::Unsupported(_))
            ));
            assert!(
                validate_options(
                    &options,
                    SinkCapability {
                        indexes: true,
                        ..capability
                    }
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn validates_shared_index_column_existence_and_uniqueness() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert!(validate_indexes(&schema, &[]).is_ok());
        assert!(validate_indexes(&schema, &[IndexSpec::new("id", IndexKind::Scalar)]).is_ok());
        assert!(matches!(
            validate_indexes(&schema, &[IndexSpec::new("missing", IndexKind::Scalar)]),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            validate_indexes(
                &schema,
                &[
                    IndexSpec::new("id", IndexKind::Scalar),
                    IndexSpec::new("id", IndexKind::BloomFilter)
                ]
            ),
            Err(Error::Unsupported(_))
        ));
    }
}
