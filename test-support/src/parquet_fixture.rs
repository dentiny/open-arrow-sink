/// Read every Parquet file in a directory in path order, including empty batches.
pub async fn read_parquet_dir(
    storage: &open_arrow_sink_storage::Storage,
    directory: &open_arrow_sink_storage::ObjectPath,
) -> Vec<arrow_array::RecordBatch> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let mut files = crate::sorted_list(storage, directory).await;
    files.retain(|file| file.as_str().ends_with(".parquet"));
    let mut batches = Vec::new();
    for file in files {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(storage.read(&file).await.unwrap()).unwrap();
        let schema = reader.schema().clone();
        let start = batches.len();
        batches.extend(reader.build().unwrap().map(Result::unwrap));
        // Retain the file's schema even when the reader produces no rows.
        if batches.len() == start {
            batches.push(arrow_array::RecordBatch::new_empty(schema));
        }
    }
    batches
}
