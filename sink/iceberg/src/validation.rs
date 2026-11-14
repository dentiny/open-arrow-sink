use iceberg::table::Table;
use open_arrow_sink_core::{Error, Result};

pub(crate) fn validate_table(table: &Table) -> Result<()> {
    if table.readonly()
        || !table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty()
    {
        return Err(Error::Unsupported(
            "only writable, unpartitioned tables are supported".into(),
        ));
    }
    Ok(())
}
