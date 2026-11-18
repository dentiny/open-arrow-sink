use std::num::NonZeroUsize;

use open_arrow_sink_core::{
    Error, Layer, Ordering, ParallelSink, Result, SchemaRef, Sink, SinkCapability, WriteOptions,
    Writer, async_trait, validation,
};

/// A pre-consumption size hint. Every known size is only a planning estimate.
/// This deliberately has no dependency on the source library's statistics types.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum InputSizeHint {
    #[default]
    Unknown,
    /// Encoded source bytes used directly as a coarse 1:1 output-size proxy.
    /// Projection, filtering, re-encoding and downloaded blobs can invalidate it.
    /// No claim of equal input/output compression is made.
    SourceBytes(u64),
    /// Estimated destination bytes, after application-specific transformations
    /// and encoding estimates. Preferred when the application can provide it.
    OutputBytes(u64),
}

/// Initial plan, available without consuming input or opening the destination.
/// File count is approximate and never controls how many rows are accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestionPlan {
    pub input_size: InputSizeHint,
    /// None for unknown size; otherwise ceil(size / target), including zero.
    /// This is a global volume estimate, not a per-partition file-count promise.
    pub estimated_files: Option<u64>,
    pub writers: NonZeroUsize,
    pub target_file_size: NonZeroUsize,
}

/// Choose write concurrency from metadata before pulling the input stream.
///
/// Defaults to at most eight writers and one writer for unknown size. Configure
/// one layer per selected COPY input. Writers rotate files using the same target;
/// actual file counts and sizes depend on encoding, partitioning and tail files.
/// A zero estimate still opens one writer and consumes the stream to EOF.
///
/// This uses the same bounded dispatcher as `UnorderedWriteLayer`: at most one
/// batch per writer is in flight, no background tasks are spawned, and encoding
/// is not made CPU-parallel. Upstream read-ahead and backend encoding/upload
/// buffers need their own limits; this is not a process memory bound.
///
/// This layer owns the target. Conflicting caller targets are rejected. Place
/// option-changing layers outside this layer; inner layers must preserve its
/// target so backend rotation and concurrency planning use the same policy.
#[derive(Debug, Clone, Copy)]
pub struct ParallelIngestionLayer {
    target_file_size: NonZeroUsize,
    input_size: InputSizeHint,
    max_writers: NonZeroUsize,
    fallback_writers: NonZeroUsize,
}

impl ParallelIngestionLayer {
    pub fn new(target_file_size: NonZeroUsize) -> Self {
        Self {
            target_file_size,
            input_size: InputSizeHint::Unknown,
            max_writers: NonZeroUsize::new(8).unwrap(),
            fallback_writers: NonZeroUsize::MIN,
        }
    }

    pub fn with_input_size(mut self, input_size: InputSizeHint) -> Self {
        self.input_size = input_size;
        self
    }

    pub fn with_max_writers(mut self, max_writers: NonZeroUsize) -> Self {
        self.max_writers = max_writers;
        self
    }

    pub fn with_fallback_writers(mut self, fallback_writers: NonZeroUsize) -> Self {
        self.fallback_writers = fallback_writers;
        self
    }

    /// Compute once from metadata; no scan, sampling or EOF is needed.
    pub fn plan(&self) -> IngestionPlan {
        let estimated_files = match self.input_size {
            InputSizeHint::Unknown => None,
            InputSizeHint::SourceBytes(bytes) | InputSizeHint::OutputBytes(bytes) => {
                // Division plus remainder avoids overflowing bytes + target - 1.
                let target = self.target_file_size.get() as u64;
                Some(bytes / target + u64::from(bytes % target != 0))
            }
        };
        let writers = estimated_files
            .unwrap_or(self.fallback_writers.get() as u64)
            .max(1)
            .min(self.max_writers.get() as u64) as usize;
        IngestionPlan {
            input_size: self.input_size,
            estimated_files,
            writers: NonZeroUsize::new(writers).unwrap(),
            target_file_size: self.target_file_size,
        }
    }
}

pub struct ParallelIngestionSink<S> {
    inner: S,
    policy: ParallelIngestionLayer,
}

impl<S: ParallelSink> Layer<S> for ParallelIngestionLayer {
    type Layered = ParallelIngestionSink<S>;

    fn layer(&self, inner: S) -> Self::Layered {
        ParallelIngestionSink {
            inner,
            policy: *self,
        }
    }
}

impl<S> ParallelIngestionSink<S> {
    /// Inspect the initial policy without opening a session.
    pub fn plan(&self) -> IngestionPlan {
        self.policy.plan()
    }
}

#[async_trait]
impl<S: ParallelSink> Sink for ParallelIngestionSink<S> {
    type Request = S::Request;

    fn capability(&self) -> SinkCapability {
        self.inner.capability().with_ordering(Ordering::Unordered)
    }

    async fn open(
        &self,
        request: Self::Request,
        schema: SchemaRef,
        mut options: WriteOptions,
    ) -> Result<Box<dyn Writer>> {
        validation::validate_ordering_options(options.ordering, Ordering::Unordered)?;
        let plan = self.plan();
        if options
            .target_file_size
            .is_some_and(|size| size != plan.target_file_size.get())
        {
            return Err(Error::InvalidConfig(
                "target_file_size conflicts with ParallelIngestionLayer".into(),
            ));
        }
        options.target_file_size = Some(plan.target_file_size.get());
        let session = self
            .inner
            .open_writers(request, schema.clone(), options, plan.writers)
            .await?
            .expect(plan.writers.get(), &schema)?;
        Ok(crate::unordered::unordered_writer(schema, session))
    }
}
