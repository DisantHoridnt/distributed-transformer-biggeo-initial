use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::{SchemaRef, Schema};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};
use futures::{Stream, StreamExt};
use pin_project_lite::pin_project;

use crate::formats::DataFormat;

pub struct FormatExecPlan {
    format: Box<dyn DataFormat + Send + Sync>,
    data: Bytes,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl FormatExecPlan {
    pub fn new(
        format: Box<dyn DataFormat + Send + Sync>,
        data: Bytes,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            format,
            data,
            schema,
            projection,
            filters,
        }
    }

    fn projected_schema(&self) -> SchemaRef {
        match &self.projection {
            Some(proj) => {
                let fields: Vec<_> = proj
                    .iter()
                    .map(|i| self.schema.field(*i).clone())
                    .collect();
                Arc::new(Schema::new(fields))
            }
            None => self.schema.clone(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for FormatExecPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    async fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        // Create a stream that applies filters and projections
        let stream = self.format
            .read_batches(self.data.clone())
            .await
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;

        Ok(Box::pin(FormatStream {
            schema: self.projected_schema(),
            stream,
            projection: self.projection.clone(),
            filters: self.filters.clone(),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for FormatExecPlan {
    fn fmt_as(&self, _t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FormatExecPlan")
    }
}

pin_project! {
    struct FormatStream {
        schema: SchemaRef,
        #[pin]
        stream: BoxStream<'static, Result<arrow::record_batch::RecordBatch>>,
        projection: Option<Vec<usize>>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
    }
}

impl Stream for FormatStream {
    type Item = datafusion::error::Result<arrow::record_batch::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Apply filters
                let mut filtered_batch = batch;
                for filter in this.filters.iter() {
                    let mask = filter
                        .evaluate(&filtered_batch)
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?
                        .into_array(filtered_batch.num_rows())
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    
                    filtered_batch = arrow::compute::filter_record_batch(&filtered_batch, &mask)
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                }

                // Apply projection
                if let Some(proj) = this.projection {
                    let projected_columns: Result<Vec<_>, _> = proj
                        .iter()
                        .map(|&i| Ok(filtered_batch.column(i).clone()))
                        .collect();
                    
                    filtered_batch = arrow::record_batch::RecordBatch::try_new(
                        this.schema.clone(),
                        projected_columns?,
                    )
                    .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                }

                Poll::Ready(Some(Ok(filtered_batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(DataFusionError::Internal(e.to_string())))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for FormatStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
