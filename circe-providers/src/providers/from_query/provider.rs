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

use crate::convert::scylla_type_to_arrow;
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
    max_concurrency: Option<usize>,
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

    pub fn num_partitions(&self) -> usize {
        if self.param_sets.is_empty() {
            1
        } else {
            let default_max = num_cpus::get() * 10;
            let max_concurrency = self.max_concurrency.unwrap_or(default_max);
            self.param_sets.len().min(max_concurrency)
        }
    }

    pub fn num_queries(&self) -> usize {
        if self.param_sets.is_empty() {
            1
        } else {
            self.param_sets.len()
        }
    }

    fn metadata_to_schema(prepared: &PreparedStatement) -> Result<Schema> {
        let col_specs_guard = prepared.get_current_result_set_col_specs();
        let col_specs = col_specs_guard.get();
        let fields: Vec<Field> = col_specs
            .iter()
            .map(|col_spec| {
                let arrow_type = scylla_type_to_arrow(col_spec.typ())?;
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
        _state: &dyn DfSession,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let default_max = num_cpus::get() * 10;
        let max_concurrency = self.max_concurrency.unwrap_or(default_max);

        let mut exec = ScyllaFromQueryExec::new(
            self.session.clone(),
            self.schema.clone(),
            self.prepared.clone(),
            self.param_sets.clone(),
            max_concurrency,
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
    param_sets: Vec<Vec<CqlValue>>,
    max_concurrency: Option<usize>,
    on_query_complete: Option<QueryCompleteCallback>,
    prepared: Option<Arc<PreparedStatement>>,
    schema: Option<SchemaRef>,
}

impl ScyllaFromQueryProviderBuilder {
    fn new(session: Arc<ScyllaSession>, query: String) -> Self {
        Self {
            session,
            query,
            param_sets: vec![],
            max_concurrency: None,
            on_query_complete: None,
            prepared: None,
            schema: None,
        }
    }

    pub fn param_sets(mut self, sets: Vec<Vec<CqlValue>>) -> Self {
        self.param_sets = sets;
        self
    }

    /// Sets the maximum total concurrency across all partitions.
    ///
    /// Defaults to `num_cpus * 10` if not specified.
    ///
    /// This caps the total number of concurrent queries to Scylla, useful on
    /// systems with many CPUs where the default would be too high.
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = Some(max);
        self
    }

    pub fn on_query_complete(mut self, cb: QueryCompleteCallback) -> Self {
        self.on_query_complete = Some(cb);
        self
    }

    pub fn prepared(&self) -> Option<&PreparedStatement> {
        self.prepared.as_deref()
    }

    pub async fn build(mut self) -> Result<ScyllaFromQueryProvider> {
        let prepared = self
            .session
            .prepare(self.query.as_str())
            .await
            .map_err(ScyllaProviderError::Prepare)?;

        let schema = ScyllaFromQueryProvider::metadata_to_schema(&prepared)?;
        self.prepared = Some(Arc::new(prepared.clone()));
        self.schema = Some(Arc::new(schema));

        #[allow(clippy::unwrap_used)]
        Ok(ScyllaFromQueryProvider {
            session: self.session,
            schema: self.schema.unwrap(),
            prepared: self.prepared.unwrap(),
            param_sets: self.param_sets,
            max_concurrency: self.max_concurrency,
            on_query_complete: self.on_query_complete,
        })
    }
}

impl fmt::Debug for ScyllaFromQueryProviderBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaFromQueryProviderBuilder")
            .field("query", &self.query)
            .field("param_sets_count", &self.param_sets.len())
            .finish()
    }
}
