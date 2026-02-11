use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::Session as DfSession;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use scylla::client::session::Session as ScyllaSession;
use scylla::statement::prepared::PreparedStatement;

use crate::convert::scylla_type_to_arrow;
use crate::error::ScyllaProviderError;

use super::exec::{RangeCompleteCallback, ScyllaTokenRangeExec};

/// A DataFusion TableProvider that reads from ScyllaDB using token range scans.
///
/// Divides the full Murmur3 token range into sub-ranges and executes concurrent
/// `SELECT * FROM <ks>.<table> WHERE TOKEN(...) >= ? AND TOKEN(...) < ? BYPASS CACHE`
/// queries to perform a full table scan.
#[derive(Clone)]
pub struct ScyllaTokenRangeProvider {
    session: Arc<ScyllaSession>,
    keyspace: String,
    table: String,
    schema: SchemaRef,
    partition_key_columns: Vec<String>,
    prepared_scan: Arc<PreparedStatement>,
    /// N = nodes * cores_per_node * 3 (max parallel queries)
    concurrency: usize,
    on_range_complete: Option<RangeCompleteCallback>,
}

impl fmt::Debug for ScyllaTokenRangeProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaTokenRangeProvider")
            .field("keyspace", &self.keyspace)
            .field("table", &self.table)
            .field("partition_key_columns", &self.partition_key_columns)
            .field("concurrency", &self.concurrency)
            .field("has_on_range_complete", &self.on_range_complete.is_some())
            .finish()
    }
}

impl ScyllaTokenRangeProvider {
    pub fn builder(
        session: Arc<ScyllaSession>,
        keyspace: String,
        table: String,
    ) -> ScyllaTokenRangeProviderBuilder {
        ScyllaTokenRangeProviderBuilder::new(session, keyspace, table)
    }

    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn partition_key_columns(&self) -> &[String] {
        &self.partition_key_columns
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn num_ranges(&self) -> usize {
        self.concurrency * 100
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
impl TableProvider for ScyllaTokenRangeProvider {
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
        let mut exec = ScyllaTokenRangeExec::new(
            self.session.clone(),
            self.schema.clone(),
            self.prepared_scan.clone(),
            self.concurrency,
            self.num_ranges(),
        );
        if let Some(indices) = projection.cloned() {
            exec = exec.with_projection(indices)?;
        }
        if let Some(cb) = &self.on_range_complete {
            exec = exec.with_on_range_complete(Arc::clone(cb));
        }
        Ok(Arc::new(exec))
    }
}

/// Builder for constructing a [`ScyllaTokenRangeProvider`].
pub struct ScyllaTokenRangeProviderBuilder {
    session: Arc<ScyllaSession>,
    keyspace: String,
    table: String,
    partition_key_columns: Option<Vec<String>>,
    columns: Vec<String>,
    concurrency: Option<usize>,
    nodes: Option<usize>,
    cores_per_node: Option<usize>,
    on_range_complete: Option<RangeCompleteCallback>,
}

impl ScyllaTokenRangeProviderBuilder {
    fn new(session: Arc<ScyllaSession>, keyspace: String, table: String) -> Self {
        Self {
            session,
            keyspace,
            table,
            partition_key_columns: None,
            columns: vec!["*".to_owned()],
            concurrency: None,
            nodes: None,
            cores_per_node: None,
            on_range_complete: None,
        }
    }

    pub fn partition_key_columns(mut self, cols: Vec<String>) -> Self {
        self.partition_key_columns = Some(cols);
        self
    }

    /// Set the columns to select. Defaults to `*` if not specified.
    pub fn columns(mut self, cols: Vec<String>) -> Self {
        self.columns = cols;
        self
    }

    /// Set the max concurrent queries directly, overriding the nodes * cores * 3 formula.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    pub fn nodes(mut self, nodes: usize) -> Self {
        self.nodes = Some(nodes);
        self
    }

    pub fn cores_per_node(mut self, cores: usize) -> Self {
        self.cores_per_node = Some(cores);
        self
    }

    pub fn on_range_complete(mut self, cb: impl Fn(usize) + Send + Sync + 'static) -> Self {
        self.on_range_complete = Some(Arc::new(cb));
        self
    }

    pub async fn build(self) -> Result<ScyllaTokenRangeProvider> {
        let partition_key_columns = self.partition_key_columns.ok_or_else(|| {
            DataFusionError::Configuration("partition_key_columns must be set".into())
        })?;

        let pk_cols_csv = partition_key_columns.join(", ");
        let select_cols = self.columns.join(", ");
        let scan_query = format!(
            "SELECT {select_cols} FROM {}.{} WHERE TOKEN({pk_cols_csv}) >= ? AND TOKEN({pk_cols_csv}) < ? BYPASS CACHE",
            self.keyspace, self.table,
        );

        let prepared_scan = self
            .session
            .prepare(scan_query)
            .await
            .map_err(ScyllaProviderError::Prepare)?;

        let schema = ScyllaTokenRangeProvider::metadata_to_schema(&prepared_scan)?;

        // Concurrency resolution: explicit > nodes*cores*3 > default(4)
        let concurrency = if let Some(c) = self.concurrency {
            c
        } else {
            match (self.nodes, self.cores_per_node) {
                (Some(nodes), Some(cores)) => nodes * cores * 3,
                _ => 4,
            }
        };
        Ok(ScyllaTokenRangeProvider {
            session: self.session,
            keyspace: self.keyspace,
            table: self.table,
            schema: Arc::new(schema),
            partition_key_columns,
            prepared_scan: Arc::new(prepared_scan),
            concurrency,
            on_range_complete: self.on_range_complete,
        })
    }
}

impl fmt::Debug for ScyllaTokenRangeProviderBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaTokenRangeProviderBuilder")
            .field("keyspace", &self.keyspace)
            .field("table", &self.table)
            .field("partition_key_columns", &self.partition_key_columns)
            .field("nodes", &self.nodes)
            .field("cores_per_node", &self.cores_per_node)
            .finish()
    }
}
