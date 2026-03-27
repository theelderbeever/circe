use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    catalog::{Session as DfSession, TableProvider},
    datasource::TableType,
    error::Result,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use scylla::{
    client::session::Session as ScyllaSession,
    serialize::row::SerializeRow,
    statement::{Consistency, prepared::PreparedStatement},
};

use crate::{convert::to_arrow, error::ScyllaProviderError};

use super::exec::{QueryCompleteCallback, ScyllaExec};

/// A DataFusion TableProvider that executes a user-provided CQL query.
///
/// Supports parameterized queries with multiple parameter sets for concurrent execution.
/// Each parameter set creates one partition, allowing DataFusion to execute queries in parallel.
#[derive(Clone)]
pub struct ScyllaProvider<P = ()> {
    session: Arc<ScyllaSession>,
    schema: SchemaRef,
    prepared: Arc<PreparedStatement>,
    param_sets: Vec<P>,
    max_concurrency: usize,
    on_query_complete: Option<QueryCompleteCallback>,
}

impl<P> fmt::Debug for ScyllaProvider<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaProvider")
            .field("param_sets_count", &self.param_sets.len())
            .finish()
    }
}

impl<P: SerializeRow + Clone + Default + Send + Sync + 'static> ScyllaProvider<P> {
    pub fn builder(session: Arc<ScyllaSession>, query: String) -> ScyllaProviderBuilder<P> {
        ScyllaProviderBuilder::new(session, query)
    }

    pub fn num_queries(&self) -> usize {
        self.param_sets.len().max(1)
    }

    fn metadata_to_schema(prepared: &PreparedStatement) -> Result<Schema> {
        let col_specs_guard = prepared.get_current_result_set_col_specs();
        let col_specs = col_specs_guard.get();
        let fields: Vec<Field> = col_specs
            .iter()
            .map(|col_spec| {
                let arrow_type = to_arrow(col_spec.typ())?;
                Ok(Field::new(col_spec.name(), arrow_type, true))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Schema::new(fields))
    }
}

#[async_trait]
impl<P: SerializeRow + Clone + Default + Send + Sync + 'static> TableProvider
    for ScyllaProvider<P>
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn DfSession,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut exec = ScyllaExec::new(
            self.session.clone(),
            self.schema.clone(),
            self.prepared.clone(),
            self.param_sets.clone(),
            state.config().target_partitions(),
            self.max_concurrency.clamp(1, self.param_sets.len().max(1)),
        );
        if let Some(cb) = &self.on_query_complete {
            exec = exec.with_on_query_complete(cb.clone());
        }
        if let Some(indices) = projection.cloned() {
            exec = exec.with_projection(indices)?;
        }
        Ok(Arc::new(exec))
    }
}

/// Builder for constructing a [`ScyllaProvider`].
pub struct ScyllaProviderBuilder<P = ()> {
    session: Arc<ScyllaSession>,
    query: String,
    params: Vec<P>,
    max_concurrency: Option<usize>,
    consistency: Option<Consistency>,
    page_size: Option<i32>,
    on_query_complete: Option<QueryCompleteCallback>,
}

impl<P: SerializeRow + Clone + Default + Send + Sync + 'static> ScyllaProviderBuilder<P> {
    fn new(session: Arc<ScyllaSession>, query: String) -> Self {
        Self {
            session,
            query,
            params: vec![],
            max_concurrency: None,
            consistency: None,
            page_size: None,
            on_query_complete: None,
        }
    }

    pub fn with_params(mut self, params: P) -> Self {
        self.params.push(params);
        self
    }

    /// Sets the maximum total concurrency across all partitions.
    ///
    /// This caps the total number of concurrent queries to Scylla, useful on
    /// systems with many CPUs where the default would be too high.
    pub fn with_max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = Some(max);
        self
    }

    pub fn with_consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = Some(consistency);
        self
    }

    pub fn with_page_size(mut self, page_size: Option<i32>) -> Self {
        self.page_size = page_size;
        self
    }

    /// A callback to run upon the completion of a query/parameter set
    pub fn on_query_complete(mut self, cb: QueryCompleteCallback) -> Self {
        self.on_query_complete = Some(cb);
        self
    }

    pub async fn build(self) -> Result<ScyllaProvider<P>> {
        let max_concurrency = self.max_concurrency.unwrap_or(self.params.len()).max(1);

        let mut prepared = self
            .session
            .prepare(self.query.as_str())
            .await
            .map_err(ScyllaProviderError::Prepare)?;

        if let Some(consistency) = self.consistency {
            prepared.set_consistency(consistency);
        }
        if let Some(page_size) = self.page_size {
            prepared.set_page_size(page_size);
        }

        let prepared = Arc::new(prepared);

        let schema = Arc::new(ScyllaProvider::<P>::metadata_to_schema(&prepared)?);

        Ok(ScyllaProvider {
            session: self.session,
            schema,
            prepared,
            param_sets: self.params,
            max_concurrency,
            on_query_complete: self.on_query_complete,
        })
    }
}

impl<P> fmt::Debug for ScyllaProviderBuilder<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaProviderBuilder")
            .field("query", &self.query)
            .field("param_sets_count", &self.params.len())
            .finish()
    }
}
