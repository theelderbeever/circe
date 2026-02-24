use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
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
use scylla::client::session::Session as ScyllaSession;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlValue, Row};

use crate::batching::BatchingStream;

/// Callback invoked when a query completes.
/// The argument is the query index within its partition.
pub type QueryCompleteCallback = Arc<dyn Fn(usize) + Send + Sync>;

/// DataFusion ExecutionPlan that executes a user-provided CQL query.
pub struct ScyllaFromQueryExec {
    session: Arc<ScyllaSession>,
    projected_schema: SchemaRef,
    full_schema: SchemaRef,
    prepared: Arc<PreparedStatement>,
    partitioned_param_sets: Vec<Vec<Vec<CqlValue>>>,
    properties: PlanProperties,
    projection: Option<Vec<usize>>,
    concurrency_per_partition: usize,
    on_query_complete: Option<QueryCompleteCallback>,
    row_counter: Arc<AtomicU64>,
}

impl fmt::Debug for ScyllaFromQueryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaFromQueryExec")
            .field("projected_schema", &self.projected_schema)
            .field("partitions", &self.partitioned_param_sets.len())
            .finish()
    }
}

impl ScyllaFromQueryExec {
    /// Calculates effective concurrency configuration based on parameter sets.
    ///
    /// The concurrency model balances parallelism with resource constraints:
    ///
    /// **DataFusion Partitions:**
    /// ```text
    /// num_partitions = min(num_param_sets, num_cpus)
    /// ```
    /// - Don't create more partitions than parameter sets (no empty partitions)
    /// - Don't create more partitions than CPUs (efficient CPU utilization)
    ///
    /// **Concurrency Per Partition:**
    /// ```text
    /// max_per_partition = max_concurrency / num_partitions
    /// concurrency_per_partition = max(1, min(max_per_partition, params_per_partition))
    /// ```
    /// - Minimum 1: Always process at least one query at a time
    /// - Maximum derived from cap: Ensures total doesn't exceed max_concurrency
    /// - Scales with workload: Uses actual params_per_partition when within bounds
    ///
    /// **Total Concurrent Queries:**
    /// ```text
    /// total = num_partitions × concurrency_per_partition ≤ max_concurrency
    /// ```
    ///
    /// **Examples (10 CPU system, max_concurrency=100):**
    /// - 5 param sets → 5 partitions × 1 buffered = 5 concurrent queries
    /// - 50 param sets → 10 partitions × 5 buffered = 50 concurrent queries
    /// - 200 param sets → 10 partitions × 10 buffered = 100 concurrent queries (capped)
    ///
    /// # Arguments
    /// * `num_param_sets` - Total number of parameter sets to execute
    /// * `max_concurrency` - Maximum total concurrent queries allowed
    ///
    /// # Returns
    /// * `(num_partitions, concurrency_per_partition)`
    fn effective_concurrency(num_param_sets: usize, max_concurrency: usize) -> (usize, usize) {
        let num_param_sets = num_param_sets.max(1);
        let num_partitions = num_param_sets.min(num_cpus::get());
        let params_per_partition = num_param_sets / num_partitions;

        // Calculate max per partition to respect total concurrency cap
        let max_per_partition = (max_concurrency / num_partitions).max(1);
        let concurrency_per_partition = 1_usize.max(max_per_partition.min(params_per_partition));

        (num_partitions, concurrency_per_partition)
    }

    pub fn new(
        session: Arc<ScyllaSession>,
        full_schema: SchemaRef,
        prepared: Arc<PreparedStatement>,
        param_sets: Vec<Vec<CqlValue>>,
        max_concurrency: usize,
        row_counter: Arc<AtomicU64>,
    ) -> Self {
        let (num_partitions, concurrency_per_partition) =
            Self::effective_concurrency(param_sets.len(), max_concurrency);

        tracing::info!(
            num_param_sets = param_sets.len(),
            num_partitions,
            concurrency_per_partition,
            total_concurrency = num_partitions * concurrency_per_partition,
            max_concurrency,
            "From-query configured"
        );

        let partitioned_param_sets = if param_sets.is_empty() {
            // No params - single partition with empty param set
            vec![vec![vec![]]]
        } else {
            // Distribute param sets round-robin across partitions
            let mut partitions: Vec<Vec<Vec<CqlValue>>> =
                (0..num_partitions).map(|_| Vec::new()).collect();

            for (i, param_set) in param_sets.into_iter().enumerate() {
                partitions[i % num_partitions].push(param_set);
            }

            partitions
        };

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
            partitioned_param_sets,
            properties,
            projection: None,
            concurrency_per_partition,
            on_query_complete: None,
            row_counter,
        }
    }

    pub fn with_on_query_complete(mut self, cb: QueryCompleteCallback) -> Self {
        self.on_query_complete = Some(cb);
        self
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
}

impl DisplayAs for ScyllaFromQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ScyllaFromQueryExec: partitions={}",
            self.partitioned_param_sets.len()
        )
    }
}

impl ExecutionPlan for ScyllaFromQueryExec {
    fn name(&self) -> &str {
        "ScyllaFromQueryExec"
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
                "ScyllaFromQueryExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let session = self.session.clone();
        let prepared = self.prepared.clone();
        let full_schema = self.full_schema.clone();
        let projection = self.projection.clone();
        let param_sets = self.partitioned_param_sets[partition].clone();
        let concurrency = self.concurrency_per_partition;
        let on_query_complete = self.on_query_complete.clone();
        let row_counter = self.row_counter.clone();

        tracing::debug!(
            partition,
            num_param_sets = param_sets.len(),
            concurrency,
            "Executing from-query partition"
        );

        // Each partition processes its parameter sets with limited concurrency
        let stream = futures::stream::once(async move {
            let row_stream = futures::stream::iter(param_sets.into_iter().enumerate())
                .map(move |(index, params)| {
                    let session = session.clone();
                    let prepared = prepared.clone();
                    let on_query_complete = on_query_complete.clone();
                    async move {
                        let row_stream = query_with_params(session, prepared, params).await?;
                        Ok::<_, DataFusionError>(OnCompleteStream::new(row_stream, move || {
                            if let Some(cb) = &on_query_complete {
                                cb(index);
                            }
                        }))
                    }
                })
                .buffered(concurrency)
                .try_flatten();

            let batched =
                BatchingStream::new(Box::pin(row_stream), full_schema, projection, row_counter);
            Ok::<_, DataFusionError>(batched)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }
}

/// Executes a query with the given parameters and returns a stream of rows.
async fn query_with_params(
    session: Arc<ScyllaSession>,
    prepared: Arc<PreparedStatement>,
    params: Vec<CqlValue>,
) -> std::result::Result<
    impl Stream<Item = std::result::Result<Row, DataFusionError>>,
    DataFusionError,
> {
    tracing::debug!(param_count = params.len(), "Starting parameterized query");

    // Execute query with parameters as a slice
    let pager = session
        .execute_iter((*prepared).clone(), params.as_slice())
        .await
        .map_err(|e| {
            tracing::error!(param_count = params.len(), error = %e, "Parameterized query failed");
            DataFusionError::External(Box::new(e))
        })?;

    let typed_stream = pager
        .rows_stream::<Row>()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(typed_stream.map_err(|e| DataFusionError::External(Box::new(e))))
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
