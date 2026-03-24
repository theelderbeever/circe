use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};
use futures::Stream;
use scylla::value::Row;

use crate::convert::rows_to_record_batch;

/// Default batch size for accumulating rows before converting to RecordBatch.
pub const DEFAULT_BATCH_SIZE: usize = 8192;

/// A stream that accumulates rows into batches and converts them to Arrow RecordBatches.
///
/// This stream buffers rows from ScyllaDB and flushes them as Arrow RecordBatches
/// when the buffer reaches the configured batch size. This is more efficient than
/// converting individual rows.
pub struct BatchingStream {
    inner: Pin<Box<dyn Stream<Item = std::result::Result<Row, DataFusionError>> + Send>>,
    buffer: Vec<Row>,
    full_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    done: bool,
    batch_size: usize,
}

impl BatchingStream {
    /// Creates a new batching stream with the default batch size.
    pub fn new(
        inner: Pin<Box<dyn Stream<Item = std::result::Result<Row, DataFusionError>> + Send>>,
        full_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self::with_batch_size(inner, full_schema, projection, DEFAULT_BATCH_SIZE)
    }

    /// Creates a new batching stream with a custom batch size.
    pub fn with_batch_size(
        inner: Pin<Box<dyn Stream<Item = std::result::Result<Row, DataFusionError>> + Send>>,
        full_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        Self {
            inner,
            buffer: Vec::with_capacity(batch_size),
            full_schema,
            projection,
            done: false,
            batch_size,
        }
    }

    fn flush_buffer(&mut self) -> Result<RecordBatch> {
        let batch = rows_to_record_batch(&self.buffer, &self.full_schema)?;
        // Increment row counter once per batch (efficient)

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
                    if this.buffer.len() >= this.batch_size {
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

impl TryInto<SendableRecordBatchStream> for BatchingStream {
    type Error = DataFusionError;
    fn try_into(
        self: BatchingStream,
    ) -> std::result::Result<SendableRecordBatchStream, Self::Error> {
        let schema = match &self.projection {
            Some(indices) => self
                .full_schema
                .clone()
                .project(indices)
                .map(Arc::new)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None)),
            None => Ok(self.full_schema.clone()),
        }?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, self)))
    }
}
