use arrow_array::cast::AsArray;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use open_arrow_sink_core::{
    Compression, CompressionOptions, Ordering, Sink, SinkExt, StructuralEncoding, WriteMode,
    WriteOptions,
};
use open_arrow_sink_lance::{LanceDatasetSink, Url};
use open_arrow_sink_layer::CompressionLayer;

fn batch(count: usize) -> RecordBatch {
    let text = Arc::new(StringArray::from_iter_values(
        (0..count).map(|i| format!("unique-{i:08}-{}", "payload ".repeat(100))),
    ));
    let nested = StructArray::from(
        RecordBatch::try_from_iter([("text", text.clone() as ArrayRef)]).unwrap(),
    );
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..count as i64)) as ArrayRef,
        ),
        ("nested", Arc::new(nested) as ArrayRef),
        ("raw", text as ArrayRef),
    ])
    .unwrap()
}

fn policy() -> CompressionOptions {
    CompressionOptions {
        global: Some(Compression::Zstd { level: Some(3) }),
        columns: [("raw".into(), Compression::Uncompressed)].into(),
        structural_encoding: Some(StructuralEncoding::Fullzip),
        column_encodings: [("raw".into(), StructuralEncoding::Miniblock)].into(),
        ..Default::default()
    }
}

#[rstest::rstest]
#[case::single_writer(false)]
#[case::parallel_writers(true)]
#[tokio::test]
async fn structural_encoding_changes_physical_pages_and_survives_rolling(#[case] parallel: bool) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let url = Url::from_directory_path(&path).unwrap();
    let input = batch(10 * 1024);
    let compression = LanceDatasetSink::default().layer(CompressionLayer::new(policy()));
    let sink: Box<dyn Sink<Request = Url>> = if parallel {
        Box::new(
            compression.layer(open_arrow_sink_layer::UnorderedWriteLayer::new(
                std::num::NonZeroUsize::new(2).unwrap(),
            )),
        )
    } else {
        Box::new(compression)
    };
    let mut writer = sink
        .open(
            url.clone(),
            input.schema(),
            WriteOptions::default()
                .with_ordering(Ordering::Unordered)
                .with_storage_version(Some("2.2".into()))
                .with_target_file_size(Some(1)),
        )
        .await
        .unwrap();
    writer.write(input.clone()).await.unwrap();
    writer.write(input.clone()).await.unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        2 * input.num_rows() as u64
    );

    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    let arrow_schema = arrow_schema::Schema::from(dataset.schema());
    let nested = arrow_schema.field_with_name("nested").unwrap();
    let arrow_schema::DataType::Struct(children) = nested.data_type() else {
        panic!("expected struct")
    };
    for field in [
        arrow_schema.field_with_name("id").unwrap(),
        nested,
        children[0].as_ref(),
    ] {
        assert_eq!(
            field.metadata()["lance-encoding:structural-encoding"],
            "fullzip"
        );
        assert_eq!(field.metadata()["lance-encoding:compression"], "zstd");
    }
    let raw = arrow_schema.field_with_name("raw").unwrap();
    assert_eq!(
        raw.metadata()["lance-encoding:structural-encoding"],
        "miniblock"
    );
    assert_eq!(raw.metadata()["lance-encoding:compression"], "none");
    let actual = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(actual.num_rows(), 2 * input.num_rows());
    let ids = actual
        .column_by_name("id")
        .unwrap()
        .as_primitive::<arrow_array::types::Int64Type>();
    let raw = actual.column_by_name("raw").unwrap().as_string::<i32>();
    let nested = actual.column_by_name("nested").unwrap().as_struct();
    let nested_text = nested.column(0).as_string::<i32>();
    let expected = input.column_by_name("raw").unwrap().as_string::<i32>();
    let mut occurrences = vec![0; input.num_rows()];
    for row in 0..actual.num_rows() {
        let id = usize::try_from(ids.value(row)).unwrap();
        assert_eq!(raw.value(row), expected.value(id));
        assert_eq!(nested_text.value(row), expected.value(id));
        occurrences[id] += 1;
    }
    assert_eq!(occurrences, vec![2; input.num_rows()]);

    assert!(dataset.get_fragments().len() >= 2);
    for pages in file_encodings(&url).await {
        assert!(
            pages.iter().any(|page| page.contains("FullZipLayout")),
            "{pages:?}"
        );
        assert!(
            pages.iter().any(|page| page.contains("MiniBlockLayout")),
            "{pages:?}"
        );
    }
}

async fn file_encodings(url: &Url) -> Vec<Vec<String>> {
    use lance_file::reader::{FileReader, describe_encoding};
    use lance_io::utils::CachedFileSize;
    use lance_io::{
        object_store::ObjectStore,
        scheduler::{ScanScheduler, SchedulerConfig},
    };

    let dataset = lance::Dataset::open(url.as_str()).await.unwrap();
    let (store, base) = ObjectStore::from_uri(url.as_str()).await.unwrap();
    let scheduler = ScanScheduler::new(store, SchedulerConfig::default_for_testing());
    let mut encodings = Vec::new();
    for fragment in dataset.get_fragments() {
        for file in &fragment.metadata().files {
            let file = scheduler
                .open_file(
                    &base.clone().join("data").join(file.path.as_str()),
                    &CachedFileSize::unknown(),
                )
                .await
                .unwrap();
            let metadata = FileReader::read_all_metadata(&file).await.unwrap();
            encodings.push(
                metadata
                    .column_metadatas
                    .iter()
                    .flat_map(|column| &column.pages)
                    .map(describe_encoding)
                    .collect(),
            );
        }
    }
    encodings
}

#[rstest::rstest]
#[case::global(None)]
#[case::column(Some(vec!["raw"]))]
#[case::nested(Some(vec!["nested", "text"]))]
#[tokio::test]
async fn lz4_policy_round_trips_and_miniblock_uses_lz4(
    #[case] path: Option<Vec<&str>>,
    #[values(StructuralEncoding::Miniblock, StructuralEncoding::Fullzip)]
    encoding: StructuralEncoding,
    #[values(false, true)] parallel: bool,
) {
    let temp = tempfile::tempdir().unwrap();
    let url = Url::from_directory_path(temp.path().join("dataset")).unwrap();
    let input = batch(10 * 1024);
    let sink = LanceDatasetSink::default();
    assert!(
        sink.capability()
            .compression
            .global
            .contains(&open_arrow_sink_core::CompressionCodec::Lz4Raw)
    );
    assert!(
        sink.capability()
            .compression
            .per_column
            .contains(&open_arrow_sink_core::CompressionCodec::Lz4Raw)
    );
    let mut policy = CompressionOptions {
        global: Some(Compression::Uncompressed),
        structural_encoding: Some(encoding),
        ..Default::default()
    };
    match path.as_deref() {
        None => policy.global = Some(Compression::Lz4Raw),
        Some([column]) => {
            policy.columns.insert((*column).into(), Compression::Lz4Raw);
        }
        Some(path) => {
            policy.nested_columns.insert(
                path.iter().map(|s| (*s).into()).collect(),
                Compression::Lz4Raw,
            );
        }
    }
    let sink: Box<dyn Sink<Request = Url>> = if parallel {
        Box::new(sink.layer(open_arrow_sink_layer::UnorderedWriteLayer::new(
            std::num::NonZeroUsize::new(2).unwrap(),
        )))
    } else {
        Box::new(sink)
    };
    let mut writer = sink
        .open(
            url.clone(),
            input.schema(),
            WriteOptions::default()
                .with_ordering(Ordering::Unordered)
                .with_compression(policy),
        )
        .await
        .unwrap();
    writer.write(input.clone()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = lance::Dataset::open(url.as_str()).await.unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    // The native encoding policy changes metadata, while preserving every value.
    assert_eq!(actual.num_rows(), input.num_rows());
    assert_eq!(actual.column(0), input.column(0));
    assert_eq!(actual.column(2), input.column(2));
    assert_eq!(
        actual.column(1).as_struct().column(0),
        input.column(1).as_struct().column(0)
    );
    let schema = actual.schema();
    let target = match path.as_deref() {
        None => schema.field_with_name("id").unwrap(),
        Some([column]) => schema.field_with_name(column).unwrap(),
        Some(_) => {
            let arrow_schema::DataType::Struct(children) =
                schema.field_with_name("nested").unwrap().data_type()
            else {
                panic!("expected struct")
            };
            children[0].as_ref()
        }
    };
    assert_eq!(target.metadata()["lance-encoding:compression"], "lz4");
    assert_eq!(
        target.metadata()["lance-encoding:dict-values-compression"],
        "lz4"
    );
    if encoding == StructuralEncoding::Miniblock {
        let pages: Vec<_> = file_encodings(&url).await.into_iter().flatten().collect();
        assert!(
            pages
                .iter()
                .any(|page| page.contains("MiniBlockLayout")
                    && page.contains("CompressionAlgorithmLz4")),
            "{pages:?}"
        );
    }
}

#[tokio::test]
async fn append_compares_effective_structural_encoding_schema() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let url = Url::from_directory_path(&path).unwrap();
    let input = batch(1024);
    let sink = LanceDatasetSink::default();
    for mode in [WriteMode::Create, WriteMode::Append] {
        let mut writer = sink
            .open(
                url.clone(),
                input.schema(),
                WriteOptions::default()
                    .with_ordering(Ordering::Unordered)
                    .with_compression(policy())
                    .with_mode(mode),
            )
            .await
            .unwrap();
        writer.write(input.clone()).await.unwrap();
        writer.commit().await.unwrap();
    }
    let mut changed = policy();
    changed.structural_encoding = Some(StructuralEncoding::Miniblock);
    assert!(matches!(
        sink.open(
            url,
            input.schema(),
            WriteOptions::default()
                .with_ordering(Ordering::Unordered)
                .with_compression(changed)
                .with_mode(WriteMode::Append)
        )
        .await,
        Err(open_arrow_sink_core::Error::SchemaMismatch)
    ));
    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 2048);
}

#[tokio::test]
async fn nested_override_preserves_sibling_policy_and_rows() {
    let input = batch(64);
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("dataset");
    let options = WriteOptions::default()
        .with_ordering(Ordering::Unordered)
        .with_compression(CompressionOptions {
            global: Some(Compression::Zstd { level: Some(3) }),
            nested_columns: [(
                vec!["nested".into(), "text".into()],
                Compression::Uncompressed,
            )]
            .into(),
            ..Default::default()
        });
    let sink = LanceDatasetSink::default();
    let mut writer = sink
        .open(
            Url::from_directory_path(&path).unwrap(),
            input.schema(),
            options,
        )
        .await
        .unwrap();
    assert_eq!(writer.schema(), &input.schema());
    writer.write(input.clone()).await.unwrap();
    writer.commit().await.unwrap();
    let dataset = lance::Dataset::open(path.to_str().unwrap()).await.unwrap();
    let actual = dataset.scan().try_into_batch().await.unwrap();
    let schema = actual.schema();
    let arrow_schema::DataType::Struct(children) = schema.field(1).data_type() else {
        panic!("struct")
    };
    assert_eq!(children[0].metadata()["lance-encoding:compression"], "none");
    assert!(
        !children[0]
            .metadata()
            .contains_key("lance-encoding:compression-level")
    );
    assert_eq!(
        schema.field(2).metadata()["lance-encoding:compression"],
        "zstd"
    );
    assert_eq!(actual.num_rows(), input.num_rows());
    let nested = actual.column(1).as_struct();
    let expected = input.column(1).as_struct();
    assert_eq!(nested.column(0), expected.column(0));
}
