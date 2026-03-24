use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::Session as DfSession;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use scylla::client::session::Session as ScyllaSession;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlValue;

use crate::convert::to_arrow;
use crate::error::ScyllaProviderError;

use super::exec::{QueryCompleteCallback, ScyllaFromQueryExec};

/// A DataFusion TableProvider that executes a user-provided CQL query.
///
/// Supports parameterized queries with multiple parameter sets for concurrent execution.
/// Each parameter set creates one partition, allowing DataFusion to execute queries in parallel.
#[derive(Clone)]
pub struct ScyllaFromQueryProvider {
    session: Arc<ScyllaSession>,
    schema: SchemaRef,
    prepared: Arc<PreparedStatement>,
    param_sets: Vec<Vec<CqlValue>>,
    max_concurrency: usize,
    on_query_complete: Option<QueryCompleteCallback>,
}

impl fmt::Debug for ScyllaFromQueryProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaFromQueryProvider")
            .field("param_sets_count", &self.param_sets.len())
            .finish()
    }
}

impl ScyllaFromQueryProvider {
    pub fn builder(session: Arc<ScyllaSession>, query: String) -> ScyllaFromQueryProviderBuilder {
        ScyllaFromQueryProviderBuilder::new(session, query)
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
impl TableProvider for ScyllaFromQueryProvider {
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
        let mut exec = ScyllaFromQueryExec::new(
            self.session.clone(),
            self.schema.clone(),
            self.prepared.clone(),
            self.param_sets.clone(),
            state.config().target_partitions(),
            self.max_concurrency.clamp(1, self.param_sets.len()),
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

/// Builder for constructing a [`ScyllaFromQueryProvider`].
pub struct ScyllaFromQueryProviderBuilder {
    session: Arc<ScyllaSession>,
    query: String,
    params: Vec<Vec<CqlValue>>,
    max_concurrency: Option<usize>,
    on_query_complete: Option<QueryCompleteCallback>,
}

impl ScyllaFromQueryProviderBuilder {
    fn new(session: Arc<ScyllaSession>, query: String) -> Self {
        Self {
            session,
            query,
            params: vec![],
            max_concurrency: None,
            on_query_complete: None,
        }
    }

    pub fn with_params(mut self, params: Vec<CqlValue>) -> Self {
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

    /// A callback to run upon the completion of a query/parameter set
    pub fn on_query_complete(mut self, cb: QueryCompleteCallback) -> Self {
        self.on_query_complete = Some(cb);
        self
    }

    pub async fn build(self) -> Result<ScyllaFromQueryProvider> {
        // If not provided limit the concurrency to the minimum between the number of cpus
        // or the quantity of parameter sets. At least 1.
        let max_concurrency = self.max_concurrency.unwrap_or(self.params.len()).max(1);

        let prepared = Arc::new(
            self.session
                .prepare(self.query.as_str())
                .await
                .map_err(ScyllaProviderError::Prepare)?,
        );

        let schema = Arc::new(ScyllaFromQueryProvider::metadata_to_schema(&prepared)?);

        Ok(ScyllaFromQueryProvider {
            session: self.session,
            schema,
            prepared,
            param_sets: self.params,
            max_concurrency,
            on_query_complete: self.on_query_complete,
        })
    }
}

impl fmt::Debug for ScyllaFromQueryProviderBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaFromQueryProviderBuilder")
            .field("query", &self.query)
            .field("param_sets_count", &self.params.len())
            .finish()
    }
}
