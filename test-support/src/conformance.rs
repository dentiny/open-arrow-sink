//! Reusable contracts for backend authors. Factories own isolated destinations;
//! readers must open the published destination, not inspect buffered writer data.
use crate::concat_sorted;
use open_arrow_sink_core::{RecordBatch, SchemaRef, Sink, WriteOptions, async_trait};

#[async_trait]
pub trait SinkFixture: Send + Sync + Sized {
    type Sink: Sink;
    async fn new() -> Self;
    fn sink(&self) -> &Self::Sink;
    fn request(&self) -> <Self::Sink as Sink>::Request;
    /// Use at least two rows and sortable scalar columns, including nulls or
    /// duplicates where supported. Complex backend-specific schemas stay local.
    fn batch(&self) -> RecordBatch;
    fn options(&self) -> WriteOptions;
    /// Return all published rows. This must succeed for an empty commit too.
    async fn read(&self) -> Vec<RecordBatch>;
    /// Check cleanup/preservation appropriate to this backend's destination.
    async fn assert_aborted(&self);
}

/// Each case receives a fresh factory instance, so Create and Append backends
/// can implement the same contracts without sharing state between cases.
pub async fn assert_conforms<F: SinkFixture>() {
    round_trip(&F::new().await).await;
    empty_commit(&F::new().await).await;
    abort(&F::new().await).await;
    schema_mismatch(&F::new().await).await;
    abort_before_write(&F::new().await).await;
    poisoned_commit(&F::new().await).await;
    repeated_batches(&F::new().await).await;
}

async fn open<F: SinkFixture>(fixture: &F) -> Box<dyn open_arrow_sink_core::Writer> {
    let batch = fixture.batch();
    let writer = fixture
        .sink()
        .open(fixture.request(), batch.schema(), fixture.options())
        .await
        .unwrap();
    assert_eq!(writer.schema(), &batch.schema());
    writer
}

fn sorted(schema: &SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    for batch in batches {
        assert_eq!(
            &batch.schema(),
            schema,
            "readback schema must match the input schema"
        );
    }
    let keys: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    concat_sorted(schema, batches, &keys).unwrap()
}

async fn round_trip<F: SinkFixture>(fixture: &F) {
    let expected = fixture.batch();
    assert!(
        expected.num_rows() >= 2,
        "fixture must exercise multiple writes"
    );
    let mut writer = open(fixture).await;
    writer.write(expected.slice(0, 0)).await.unwrap();
    writer.write(expected.slice(0, 1)).await.unwrap();
    writer
        .write(expected.slice(1, expected.num_rows() - 1))
        .await
        .unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        expected.num_rows() as u64
    );
    assert_eq!(
        sorted(&expected.schema(), &fixture.read().await),
        sorted(&expected.schema(), &[expected])
    );
}

async fn empty_commit<F: SinkFixture>(fixture: &F) {
    assert_eq!(open(fixture).await.commit().await.unwrap().rows_written, 0);
    let expected = fixture.batch();
    let actual = sorted(&expected.schema(), &fixture.read().await);
    assert_eq!(actual, expected.slice(0, 0));
}

async fn abort<F: SinkFixture>(fixture: &F) {
    let mut writer = open(fixture).await;
    writer.write(fixture.batch()).await.unwrap();
    writer.abort().await.unwrap();
    fixture.assert_aborted().await;
}

async fn schema_mismatch<F: SinkFixture>(fixture: &F) {
    let mut writer = open(fixture).await;
    let schema = std::sync::Arc::new(arrow_schema::Schema::empty());
    let wrong = RecordBatch::new_empty(schema);
    assert!(writer.write(wrong).await.is_err());
    assert!(
        writer.write(fixture.batch()).await.is_err(),
        "schema error must be terminal"
    );
    writer.abort().await.unwrap();
    fixture.assert_aborted().await;
}

async fn abort_before_write<F: SinkFixture>(fixture: &F) {
    open(fixture).await.abort().await.unwrap();
    fixture.assert_aborted().await;
}

async fn poisoned_commit<F: SinkFixture>(fixture: &F) {
    let mut writer = open(fixture).await;
    writer.write(fixture.batch()).await.unwrap();
    let wrong = RecordBatch::new_empty(std::sync::Arc::new(arrow_schema::Schema::empty()));
    assert!(writer.write(wrong).await.is_err());
    assert!(
        writer.commit().await.is_err(),
        "a poisoned writer cannot publish accepted rows"
    );
    fixture.assert_aborted().await;
}

async fn repeated_batches<F: SinkFixture>(fixture: &F) {
    let expected = fixture.batch();
    let mut writer = open(fixture).await;
    for _ in 0..3 {
        writer.write(expected.clone()).await.unwrap();
    }
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        3 * expected.num_rows() as u64
    );
    assert_eq!(
        sorted(&expected.schema(), &fixture.read().await),
        sorted(
            &expected.schema(),
            &[expected.clone(), expected.clone(), expected]
        )
    );
}
