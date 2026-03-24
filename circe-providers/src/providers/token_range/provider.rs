// use std::any::Any;
// use std::fmt;
// use std::sync::Arc;
// use std::sync::atomic::AtomicU64;

// use async_trait::async_trait;
// use datafusion::arrow::datatypes::SchemaRef;
// use datafusion::catalog::Session as DfSession;
// use datafusion::catalog::TableProvider;
// use datafusion::datasource::TableType;
// use datafusion::error::{DataFusionError, Result};
// use datafusion::logical_expr::Expr;
// use datafusion::physical_plan::ExecutionPlan;
// use scylla::client::session::Session as ScyllaSession;
// use scylla::value::CqlValue;

// use crate::convert::to_arrow;
// use crate::error::ScyllaProviderError;
// use crate::providers::from_query::{QueryCompleteCallback, ScyllaFromQueryProvider};

// use super::range::TokenRange;

// /// Callback invoked when a token range query completes.
// /// The argument is the range index.
// pub type RangeCompleteCallback = Arc<dyn Fn(usize) + Send + Sync>;

// /// A DataFusion TableProvider that reads from ScyllaDB using token range scans.
// ///
// /// Splits the Murmur3 token space into `concurrency` equal ranges and executes
// /// them as concurrent `SELECT ... WHERE TOKEN(...) >= ? AND TOKEN(...) < ? BYPASS CACHE`
// /// queries via [`ScyllaFromQueryProvider`].
// #[derive(Clone)]
// pub struct ScyllaTokenRangeProvider {
//     inner: ScyllaFromQueryProvider,
//     keyspace: String,
//     table: String,
//     partition_key_columns: Vec<String>,
//     concurrency: usize,
// }

// impl fmt::Debug for ScyllaTokenRangeProvider {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("ScyllaTokenRangeProvider")
//             .field("keyspace", &self.keyspace)
//             .field("table", &self.table)
//             .field("partition_key_columns", &self.partition_key_columns)
//             .field("concurrency", &self.concurrency)
//             .finish()
//     }
// }

// impl ScyllaTokenRangeProvider {
//     pub fn builder(
//         session: Arc<ScyllaSession>,
//         keyspace: String,
//         table: String,
//     ) -> ScyllaTokenRangeProviderBuilder {
//         ScyllaTokenRangeProviderBuilder::new(session, keyspace, table)
//     }

//     pub fn keyspace(&self) -> &str {
//         &self.keyspace
//     }

//     pub fn table(&self) -> &str {
//         &self.table
//     }

//     pub fn partition_key_columns(&self) -> &[String] {
//         &self.partition_key_columns
//     }

//     pub fn concurrency(&self) -> usize {
//         self.concurrency
//     }

//     pub fn num_ranges(&self) -> usize {
//         self.concurrency
//     }

//     // pub fn row_counter(&self) -> Arc<AtomicU64> {
//     //     self.inner.row_counter()
//     //     Arc::clone(&self.row_counter)
//     // }
// }

// #[async_trait]
// impl TableProvider for ScyllaTokenRangeProvider {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }

//     fn schema(&self) -> SchemaRef {
//         self.inner.schema()
//     }

//     fn table_type(&self) -> TableType {
//         TableType::Base
//     }

//     async fn scan(
//         &self,
//         state: &dyn DfSession,
//         projection: Option<&Vec<usize>>,
//         filters: &[Expr],
//         limit: Option<usize>,
//     ) -> Result<Arc<dyn ExecutionPlan>> {
//         self.inner.scan(state, projection, filters, limit).await
//     }
// }

// /// Builder for constructing a [`ScyllaTokenRangeProvider`].
// pub struct ScyllaTokenRangeProviderBuilder {
//     session: Arc<ScyllaSession>,
//     keyspace: String,
//     table: String,
//     partition_key_columns: Option<Vec<String>>,
//     columns: Vec<String>,
//     concurrency: Option<usize>,
//     on_range_complete: Option<RangeCompleteCallback>,
// }

// impl ScyllaTokenRangeProviderBuilder {
//     fn new(session: Arc<ScyllaSession>, keyspace: String, table: String) -> Self {
//         Self {
//             session,
//             keyspace,
//             table,
//             partition_key_columns: None,
//             columns: vec!["*".to_owned()],
//             concurrency: None,
//             on_range_complete: None,
//         }
//     }

//     pub fn partition_key_columns(mut self, cols: Vec<String>) -> Self {
//         self.partition_key_columns = Some(cols);
//         self
//     }

//     /// Set the columns to select. Defaults to `*` if not specified.
//     pub fn columns(mut self, cols: Vec<String>) -> Self {
//         self.columns = cols;
//         self
//     }

//     /// Set the number of token range splits and max concurrent queries.
//     /// The token space is divided into exactly `concurrency` ranges.
//     pub fn concurrency(mut self, concurrency: usize) -> Self {
//         self.concurrency = Some(concurrency);
//         self
//     }

//     pub fn on_range_complete(mut self, cb: impl Fn(usize) + Send + Sync + 'static) -> Self {
//         self.on_range_complete = Some(Arc::new(cb));
//         self
//     }

//     pub async fn build(self) -> Result<ScyllaTokenRangeProvider> {
//         let partition_key_columns = self.partition_key_columns.ok_or_else(|| {
//             DataFusionError::Configuration("partition_key_columns must be set".into())
//         })?;

//         let concurrency = self
//             .concurrency
//             .ok_or_else(|| DataFusionError::Configuration("concurrency must be set".into()))?;

//         let pk_cols_csv = partition_key_columns.join(", ");
//         let select_cols = self.columns.join(", ");
//         let scan_query = format!(
//             "SELECT {select_cols} FROM {}.{} WHERE TOKEN({pk_cols_csv}) >= ? AND TOKEN({pk_cols_csv}) < ? BYPASS CACHE",
//             self.keyspace, self.table,
//         );

//         let param_sets: Vec<Vec<CqlValue>> = TokenRange::split(concurrency)
//             .map(|r| vec![CqlValue::BigInt(r.start), CqlValue::BigInt(r.end)])
//             .collect();

//         let mut from_query_builder = ScyllaFromQueryProvider::builder(self.session, scan_query)
//             .param_sets(param_sets)
//             .max_concurrency(concurrency);

//         if let Some(cb) = self.on_range_complete {
//             let callback: QueryCompleteCallback = cb;
//             from_query_builder = from_query_builder.on_query_complete(callback);
//         }

//         let inner = from_query_builder.build().await?;

//         Ok(ScyllaTokenRangeProvider {
//             inner,
//             keyspace: self.keyspace,
//             table: self.table,
//             partition_key_columns,
//             concurrency,
//         })
//     }
// }

// impl fmt::Debug for ScyllaTokenRangeProviderBuilder {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("ScyllaTokenRangeProviderBuilder")
//             .field("keyspace", &self.keyspace)
//             .field("table", &self.table)
//             .field("partition_key_columns", &self.partition_key_columns)
//             .field("concurrency", &self.concurrency)
//             .finish()
//     }
// }
