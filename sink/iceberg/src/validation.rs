use iceberg::table::Table;
use open_arrow_sink_core::Result;

use crate::error::unsupported_table;

pub(crate) fn validate_table(table: &Table) -> Result<()> {
    if table.readonly()
        || !table
            .metadata()
            .default_partition_spec()
            .fields()
            .is_empty()
    {
        return Err(unsupported_table());
    }
    Ok(())
}
