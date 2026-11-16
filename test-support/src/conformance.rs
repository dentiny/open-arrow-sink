//! Reusable contracts for backend authors. Factories own isolated destinations;
//! readers must open the published destination, not inspect buffered writer data.
use crate::concat_sorted;
use open_arrow_sink_core::{
    Error, RecordBatch, SchemaRef, Sink, WriteMode, WriteOptions, async_trait,
};

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
    /// Input large enough to cross this backend's page buffers when rolling.
    fn rolling_batch(&self) -> RecordBatch {
        self.batch()
    }
    async fn assert_rolled(&self);
    /// Seed and verify an unrelated object next to the destination.
    async fn create_sibling(&self);
    async fn assert_sibling(&self);
    /// Return all published rows. This must succeed for an empty commit too.
    async fn read(&self) -> Vec<RecordBatch>;
    /// Check cleanup/preservation appropriate to this backend's destination.
    async fn assert_aborted(&self);
}

/// Independently runnable cases from the shared sink contract suite.
#[derive(Clone, Copy, Debug)]
pub enum Case {
    RoundTrip,
    Rolling,
    UnsupportedModes,
    EmptyCommit,
    Abort,
    SchemaMismatch,
    AbortBeforeWrite,
    PoisonedCommit,
}
impl Case {
    pub const ALL: [Self; 8] = [
        Self::RoundTrip,
        Self::Rolling,
        Self::UnsupportedModes,
        Self::EmptyCommit,
        Self::Abort,
        Self::SchemaMismatch,
        Self::AbortBeforeWrite,
        Self::PoisonedCommit,
    ];
}

/// Run one named contract using isolated destinations from the fixture factory.
pub async fn assert_case<F: SinkFixture>(case: Case) {
    match case {
        Case::RoundTrip => round_trip(&F::new().await).await,
        Case::Rolling => rolling(&F::new().await).await,
        Case::UnsupportedModes => unsupported_modes::<F>().await,
        Case::EmptyCommit => empty_commit(&F::new().await).await,
        Case::Abort => abort(&F::new().await).await,
        Case::SchemaMismatch => schema_mismatch(&F::new().await).await,
        Case::AbortBeforeWrite => abort_before_write(&F::new().await).await,
        Case::PoisonedCommit => poisoned_commit(&F::new().await).await,
    }
}

/// Run the entire suite. Use `assert_case` when the test framework should report
/// each contract separately, or individual helpers with a custom fixture instance.
pub async fn assert_conforms<F: SinkFixture>() {
    for case in Case::ALL {
        assert_case::<F>(case).await;
    }
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

/// Verify empty and nonempty writes publish every row with the input schema.
pub async fn round_trip<F: SinkFixture>(fixture: &F) {
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

/// Verify an empty commit publishes a readable empty result.
pub async fn empty_commit<F: SinkFixture>(fixture: &F) {
    assert_eq!(open(fixture).await.commit().await.unwrap().rows_written, 0);
    let expected = fixture.batch();
    let actual = sorted(&expected.schema(), &fixture.read().await);
    assert_eq!(actual, expected.slice(0, 0));
}

/// Verify abort preserves unrelated sibling objects and the backend abort state.
pub async fn abort<F: SinkFixture>(fixture: &F) {
    fixture.create_sibling().await;
    let mut writer = open(fixture).await;
    writer.write(fixture.batch()).await.unwrap();
    writer.abort().await.unwrap();
    fixture.assert_aborted().await;
    fixture.assert_sibling().await;
}

/// Verify schema errors are terminal and the session remains abortable.
pub async fn schema_mismatch<F: SinkFixture>(fixture: &F) {
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

/// Verify a session can abort before accepting any input.
pub async fn abort_before_write<F: SinkFixture>(fixture: &F) {
    open(fixture).await.abort().await.unwrap();
    fixture.assert_aborted().await;
}

/// Verify a poisoned session cannot publish accepted input.
pub async fn poisoned_commit<F: SinkFixture>(fixture: &F) {
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

/// Verify a tiny file target produces multiple files and preserves every row.
pub async fn rolling<F: SinkFixture>(fixture: &F) {
    let expected = fixture.rolling_batch();
    let mut writer = fixture
        .sink()
        .open(
            fixture.request(),
            expected.schema(),
            fixture.options().with_target_file_size(1),
        )
        .await
        .unwrap();
    writer.write(expected.clone()).await.unwrap();
    writer.write(expected.clone()).await.unwrap();
    assert_eq!(
        writer.commit().await.unwrap().rows_written,
        2 * expected.num_rows() as u64
    );
    assert_eq!(
        sorted(&expected.schema(), &fixture.read().await),
        sorted(&expected.schema(), &[expected.clone(), expected])
    );
    fixture.assert_rolled().await;
}

/// Verify all unadvertised modes reject without changing committed content.
pub async fn unsupported_modes<F: SinkFixture>() {
    let capability = F::new().await.sink().capability();
    for mode in [WriteMode::Create, WriteMode::Append, WriteMode::Overwrite] {
        if capability.supported_modes.contains(&mode) {
            continue;
        }
        let fixture = F::new().await;
        round_trip(&fixture).await;
        let before = sorted(&fixture.batch().schema(), &fixture.read().await);
        let result = fixture
            .sink()
            .open(
                fixture.request(),
                fixture.batch().schema(),
                fixture.options().with_mode(mode),
            )
            .await;
        assert!(
            matches!(result, Err(Error::Unsupported(_))),
            "unadvertised mode {mode:?} must return Unsupported"
        );
        assert_eq!(
            sorted(&fixture.batch().schema(), &fixture.read().await),
            before
        );
    }
}
