use std::any::Any;
use std::convert::TryInto;
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
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt, TryStreamExt};
use scylla::client::session::Session as ScyllaSession;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::{CqlValue, Row};
use tokio::sync::Semaphore;

use crate::batching::BatchingStream;

/// Callback invoked when a query completes.
/// The argument is the query index within its partition.
pub type QueryCompleteCallback = Arc<dyn Fn(usize) + Send + Sync>;

/// DataFusion ExecutionPlan that executes a user-provided CQL query.
pub struct ScyllaFromQueryExec {
    session: Arc<ScyllaSession>,
    prepared: Arc<PreparedStatement>,
    partitioned_params: Vec<Vec<Vec<CqlValue>>>,
    semaphore: Arc<Semaphore>,
    schema: SchemaRef,
    properties: PlanProperties,
    projection: Option<Vec<usize>>,
    on_query_complete: Option<QueryCompleteCallback>,
}

impl fmt::Debug for ScyllaFromQueryExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaFromQueryExec")
            .field("schema", &self.schema)
            .field("concurrency", &self.semaphore.available_permits())
            .field("partitions", &self.properties.partitioning)
            .finish()
    }
}

impl ScyllaFromQueryExec {
    pub fn new(
        session: Arc<ScyllaSession>,
        schema: SchemaRef,
        prepared: Arc<PreparedStatement>,
        params: Vec<Vec<CqlValue>>,
        num_partitions: usize,
        concurrency: usize,
    ) -> Self {
        let num_partitions = num_partitions.max(1);
        let semaphore = Arc::new(Semaphore::new(concurrency));

        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        let chunk_size = params.len().div_ceil(num_partitions);
        let partitioned_params = params.chunks(chunk_size).map(|s| s.to_vec()).collect();

        Self {
            session,
            partitioned_params,
            prepared,
            semaphore,
            schema,
            properties,
            projection: None,
            on_query_complete: None,
        }
    }

    pub fn with_on_query_complete(mut self, cb: QueryCompleteCallback) -> Self {
        self.on_query_complete = Some(cb);
        self
    }

    pub fn with_projection(mut self, projection: Vec<usize>) -> Result<Self> {
        let projected_schema = Arc::new(self.schema.project(&projection)?);
        self.properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            self.properties.partitioning,
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
            self.properties.partitioning
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
        let full_schema = self.schema.clone();
        let projection = self.projection.clone();
        let semaphore = self.semaphore.clone();

        let on_query_complete = self.on_query_complete.clone();

        // tracing::debug!(
        //     partition,
        //     num_param_sets = param_sets.len(),
        //     concurrency,
        //     "Executing from-query partition"
        // );

        let params = self
            .partitioned_params
            .get(partition)
            .cloned()
            .unwrap_or_default();

        // Each partition processes its parameter sets with limited concurrency
        let row_stream = futures::stream::iter(params.into_iter().enumerate())
            .map(move |(index, params)| {
                let session = session.clone();
                let prepared = prepared.clone();
                let on_query_complete = on_query_complete.clone();
                let semaphore = semaphore.clone();
                async move {
                    let _permit = semaphore.acquire().await;
                    let row_stream = query_with_params(session, prepared, params).await?;
                    Ok::<_, DataFusionError>(OnCompleteStream::new(row_stream, move || {
                        if let Some(cb) = &on_query_complete {
                            cb(index);
                        }
                    }))
                }
            })
            .buffer_unordered(10)
            .try_flatten();

        BatchingStream::new(
            Box::pin(row_stream),
            full_schema.clone(),
            projection.clone(),
        )
        .try_into()
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
