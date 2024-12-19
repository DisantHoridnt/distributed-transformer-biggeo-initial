use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use futures::{ready, Stream, StreamExt};

use crate::formats::DataFormat;

pub struct FormatTableProvider {
    format: Box<dyn DataFormat + Send + Sync>,
    schema: SchemaRef,
    data: Pin<Box<dyn Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + Sync + 'static>>,
}

impl FormatTableProvider {
    pub fn new(
        format: Box<dyn DataFormat + Send + Sync>,
        schema: SchemaRef,
        data: Pin<Box<dyn Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            format,
            schema,
            data,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for FormatTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let data = Box::pin(futures::stream::once(futures::future::ready(Ok(RecordBatch::new_empty(self.schema.clone())))));
        let exec = FormatExecPlan::new(
            data,
            self.schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
        );
        Ok(Arc::new(exec))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}

pub struct FormatExecPlan {
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send + Sync + 'static>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl std::fmt::Debug for FormatExecPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FormatExecPlan")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .finish()
    }
}

impl FormatExecPlan {
    pub fn new(
        stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send + Sync + 'static>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            stream,
            schema,
            projection,
            filters,
            limit,
        }
    }
}

impl Clone for FormatExecPlan {
    fn clone(&self) -> Self {
        Self {
            stream: Box::pin(futures::stream::empty()),
            schema: self.schema.clone(),
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
        }
    }
}

impl DisplayAs for FormatExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FormatExecPlan")
    }
}

impl ExecutionPlan for FormatExecPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let stream = Box::pin(futures::stream::once(futures::future::ready(Ok(RecordBatch::new_empty(self.schema.clone())))));
        let stream = FormatStream::new(
            stream,
            self.schema.clone(),
            self.projection.clone(),
            self.filters.clone(),
            self.limit,
        );
        Ok(Box::pin(stream))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

pub struct FormatStream {
    inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send + Sync + 'static>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    count: usize,
}

impl FormatStream {
    fn new(
        inner: Pin<Box<dyn Stream<Item = Result<RecordBatch, DataFusionError>> + Send + Sync + 'static>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            inner,
            schema,
            projection,
            filters,
            limit,
            count: 0,
        }
    }
}

impl Stream for FormatStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(limit) = self.limit {
            if self.count >= limit {
                return Poll::Ready(None);
            }
        }

        let result = ready!(self.inner.as_mut().poll_next(cx));
        match result {
            Some(Ok(batch)) => {
                let mut filtered_batch = batch;

                // Apply filters
                for _filter in &self.filters {
                    // We don't support runtime filter evaluation yet
                    // Just pass through the data for now
                }

                // Apply projection
                if let Some(ref projection) = self.projection {
                    filtered_batch = filtered_batch
                        .project(projection)
                        .map_err(|e| DataFusionError::Execution(format!("Error projecting batch: {}", e)))?;
                }

                self.count += filtered_batch.num_rows();
                Poll::Ready(Some(Ok(filtered_batch)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for FormatStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
