//! Mandatory shared contracts, separate from optional business validation layers.
use crate::{Error, Ordering, RecordBatch, Result, SinkCapability, WriteOptions};
use arrow_schema::Schema;

/// Check declared capabilities before opening a destination or producing side effects.
pub fn validate_options(options: &WriteOptions, capability: SinkCapability) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRef;
    use arrow_schema::{DataType, Field};
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn validates_ordering() {
        for guaranteed_ordering in [Ordering::Unordered, Ordering::PreserveInput] {
            let cap = SinkCapability {
                ordering: guaranteed_ordering,
            };
            for ordering in [Ordering::PreserveInput, Ordering::Unordered] {
                assert_eq!(
                    validate_options(&WriteOptions { ordering }, cap).is_ok(),
                    ordering == Ordering::Unordered
                        || guaranteed_ordering == Ordering::PreserveInput,
                );
            }
        }
    }

    #[test]
    fn schema_contract_checks_types_nullability_order_and_metadata() {
        let a = Field::new("a", DataType::Int64, false);
        let b = Field::new("b", DataType::Utf8, true);
        let schema: SchemaRef = Arc::new(Schema::new(vec![a.clone(), b.clone()]));
        let batch = RecordBatch::new_empty(schema.clone());
        assert!(validate_batch(&schema, &batch).is_ok());
        for changed in [
            Schema::new(vec![b.clone(), a.clone()]),
            Schema::new(vec![Field::new("a", DataType::Int32, false), b.clone()]),
            Schema::new(vec![Field::new("a", DataType::Int64, true), b]),
            schema
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([("key".into(), "value".into())])),
        ] {
            assert!(matches!(
                validate_batch(&changed, &batch),
                Err(Error::SchemaMismatch)
            ));
        }
    }
}
