use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use scylla::client::session::Session as ScyllaSession;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::Row;

/// Callback invoked when a token range query completes.
/// The argument is the range index.
pub type RangeCompleteCallback = Arc<dyn Fn(usize) + Send + Sync>;

// Re-export BatchingStream for use by from_query
pub(in crate::providers) use batching::BatchingStream;

/// A token range within the Murmur3 hash space.
#[derive(Debug, Clone)]
pub struct TokenRange {
    pub start: i64,
    pub end: i64,
    pub index: usize,
}

impl TokenRange {
    /// Splits the full Murmur3 token space into `num_ranges` sub-ranges.
    ///
    /// Returns a lazy iterator of [`TokenRange`] values covering
    /// `i64::MIN..=i64::MAX`.
    pub fn split(num_ranges: usize) -> impl Iterator<Item = TokenRange> {
        const MIN_TOKEN: i128 = i64::MIN as i128;
        const MAX_TOKEN: i128 = i64::MAX as i128;

        let total_range = (MAX_TOKEN - MIN_TOKEN) as usize;
        let step = total_range / num_ranges;

        (MIN_TOKEN..=MAX_TOKEN)
            .step_by(step)
            .tuple_windows()
            .enumerate()
            .map(|(i, (start, end))| TokenRange {
                start: start.max(MIN_TOKEN) as i64,
                end: end.min(MAX_TOKEN) as i64,
                index: i,
            })
    }
}

/// Distributes token ranges round-robin across `num_partitions` groups.
fn partition_ranges(num_ranges: usize, num_partitions: usize) -> Vec<Vec<TokenRange>> {
    let mut partitions: Vec<Vec<TokenRange>> = (0..num_partitions).map(|_| Vec::new()).collect();
    for range in TokenRange::split(num_ranges) {
        partitions[range.index % num_partitions].push(range);
    }
    partitions
}

/// DataFusion ExecutionPlan that reads from ScyllaDB using concurrent token range queries.
pub struct ScyllaTokenRangeExec {
    session: Arc<ScyllaSession>,
    projected_schema: SchemaRef,
    full_schema: SchemaRef,
    prepared: Arc<PreparedStatement>,
    partitioned_ranges: Vec<Vec<TokenRange>>,
    properties: PlanProperties,
    projection: Option<Vec<usize>>,
    on_range_complete: Option<RangeCompleteCallback>,
    concurrency_per_partition: usize,
}

impl fmt::Debug for ScyllaTokenRangeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaTokenRangeExec")
            .field("projected_schema", &self.projected_schema)
            .field("partitions", &self.partitioned_ranges.len())
            .field("has_on_range_complete", &self.on_range_complete.is_some())
            .finish()
    }
}

impl ScyllaTokenRangeExec {
    pub fn new(
        session: Arc<ScyllaSession>,
        full_schema: SchemaRef,
        prepared: Arc<PreparedStatement>,
        concurrency: usize,
        num_ranges: usize,
    ) -> Self {
        // Use num_cpus for DataFusion partitions, distribute concurrency across them
        let num_partitions = num_cpus::get();
        let concurrency_per_partition = (concurrency / num_partitions).max(1);

        let partitioned_ranges = partition_ranges(num_ranges, num_partitions);

        tracing::info!(
            num_ranges,
            num_partitions,
            concurrency_per_partition,
            total_concurrency = num_partitions * concurrency_per_partition,
            "Token range scan configured"
        );

        let properties = PlanProperties::new(
            EquivalenceProperties::new(full_schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            session,
            projected_schema: full_schema.clone(),
            full_schema,
            prepared,
            partitioned_ranges,
            properties,
            projection: None,
            on_range_complete: None,
            concurrency_per_partition,
        }
    }

    pub fn with_projection(mut self, projection: Vec<usize>) -> Result<Self> {
        self.projected_schema = Arc::new(self.full_schema.project(&projection)?);
        self.properties = PlanProperties::new(
            EquivalenceProperties::new(self.projected_schema.clone()),
            self.properties.partitioning.clone(),
            self.properties.emission_type,
            self.properties.boundedness,
        );
        self.projection = Some(projection);
        Ok(self)
    }

    pub fn with_on_range_complete(mut self, cb: RangeCompleteCallback) -> Self {
        self.on_range_complete = Some(cb);
        self
    }
}

impl DisplayAs for ScyllaTokenRangeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ScyllaTokenRangeExec: partitions={}",
            self.partitioned_ranges.len()
        )
    }
}

impl ExecutionPlan for ScyllaTokenRangeExec {
    fn name(&self) -> &str {
        "ScyllaTokenRangeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "ScyllaTokenRangeExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let ranges = self.partitioned_ranges[partition].clone();
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        let full_schema = self.full_schema.clone();
        let projection = self.projection.clone();
        let on_range_complete = self.on_range_complete.clone();
        let concurrency = self.concurrency_per_partition;

        tracing::debug!(
            partition,
            num_ranges = ranges.len(),
            concurrency,
            "Executing token range partition"
        );

        let stream = futures::stream::once(async move {
            let row_stream = futures::stream::iter(ranges)
                .map(move |range| {
                    let session = session.clone();
                    let prepared = prepared.clone();
                    let on_range_complete = on_range_complete.clone();
                    async move {
                        let row_stream =
                            query_token_range(session, prepared, range.start, range.end).await?;

                        let index = range.index;
                        Ok::<_, DataFusionError>(OnCompleteStream::new(row_stream, move || {
                            if let Some(cb) = &on_range_complete {
                                cb(index);
                            }
                        }))
                    }
                })
                .buffered(concurrency)
                .try_flatten();

            let batched = BatchingStream::new(Box::pin(row_stream), full_schema, projection);
            Ok::<_, DataFusionError>(batched)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }
}

/// Executes a single token range query and returns a stream of rows.
async fn query_token_range(
    session: Arc<ScyllaSession>,
    prepared: Arc<PreparedStatement>,
    start: i64,
    end: i64,
) -> std::result::Result<
    impl Stream<Item = std::result::Result<Row, DataFusionError>>,
    DataFusionError,
> {
    tracing::debug!(start, end, "Starting token range query");

    let pager = session
        .execute_iter((*prepared).clone(), (start, end))
        .await
        .map_err(|e| {
            tracing::error!(start, end, error = %e, "Token range query failed");
            DataFusionError::External(Box::new(e))
        })?;

    let typed_stream = pager
        .rows_stream::<Row>()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(typed_stream.map_err(|e| DataFusionError::External(Box::new(e))))
}

mod batching {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::error::{DataFusionError, Result};
    use futures::Stream;
    use scylla::value::Row;

    use crate::convert::rows_to_record_batch;

    const BATCH_SIZE: usize = 8192;

    /// A stream that accumulates rows into batches of `BATCH_SIZE` and converts
    /// them to Arrow RecordBatches with optional projection.
    pub(in crate::providers) struct BatchingStream {
        inner: Pin<Box<dyn Stream<Item = std::result::Result<Row, DataFusionError>> + Send>>,
        buffer: Vec<Row>,
        full_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        done: bool,
    }

    impl BatchingStream {
        pub(in crate::providers) fn new(
            inner: Pin<Box<dyn Stream<Item = std::result::Result<Row, DataFusionError>> + Send>>,
            full_schema: SchemaRef,
            projection: Option<Vec<usize>>,
        ) -> Self {
            Self {
                inner,
                buffer: Vec::with_capacity(BATCH_SIZE),
                full_schema,
                projection,
                done: false,
            }
        }

        fn flush_buffer(&mut self) -> Result<RecordBatch> {
            let batch = rows_to_record_batch(&self.buffer, &self.full_schema)?;
            self.buffer.clear();

            match &self.projection {
                Some(indices) => batch
                    .project(indices)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None)),
                None => Ok(batch),
            }
        }
    }

    impl Stream for BatchingStream {
        type Item = Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();

            if this.done {
                return Poll::Ready(None);
            }

            loop {
                match this.inner.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(row))) => {
                        this.buffer.push(row);
                        if this.buffer.len() >= BATCH_SIZE {
                            return Poll::Ready(Some(this.flush_buffer()));
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        this.done = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        this.done = true;
                        if this.buffer.is_empty() {
                            return Poll::Ready(None);
                        }
                        return Poll::Ready(Some(this.flush_buffer()));
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

/// A stream wrapper that invokes a callback when the inner stream ends.
struct OnCompleteStream<S, F> {
    inner: S,
    on_complete: Option<F>,
}

impl<S, F> OnCompleteStream<S, F> {
    fn new(inner: S, on_complete: F) -> Self {
        Self {
            inner,
            on_complete: Some(on_complete),
        }
    }
}

impl<S, F> Stream for OnCompleteStream<S, F>
where
    S: Stream + Unpin,
    F: FnOnce() + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(None) => {
                if let Some(cb) = this.on_complete.take() {
                    cb();
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}
