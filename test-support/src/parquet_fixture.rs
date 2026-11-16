/// Read every Parquet file in a directory in path order, including empty batches.
pub async fn read_parquet_dir(
    storage: &open_arrow_sink_storage::Storage,
    directory: &open_arrow_sink_storage::Url,
) -> Vec<arrow_array::RecordBatch> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let directory = storage.resolve(directory).unwrap();
    let mut files = storage.list(&directory).await.unwrap();
    files.retain(|file| file.as_str().ends_with(".parquet"));
    files.sort_by(|a, b| a.as_str().cmp(b.as_str()));
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
