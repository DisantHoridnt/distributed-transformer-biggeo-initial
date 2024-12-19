use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Result;
use arrow::array::{Array, BooleanArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::stream::Stream;
use futures::ready;

pub struct FormatExecPlan {
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + Sync + 'static>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
}

impl std::fmt::Debug for FormatExecPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FormatExecPlan")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

impl FormatExecPlan {
    pub fn new(
        stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + Sync + 'static>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
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

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "Invalid partition {partition}"
            )));
        }

        Ok(Box::pin(FormatStream {
            schema: self.schema.clone(),
            stream: Box::pin(futures::stream::once(futures::future::ready(Ok(RecordBatch::new_empty(self.schema.clone()))))),
            projection: self.projection.clone(),
            filters: self.filters.clone(),
            limit: self.limit,
            count: 0,
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for FormatExecPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "FormatExecPlan")
            }
        }
    }
}

pub struct FormatStream {
    schema: SchemaRef,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + Sync + 'static>>,
    projection: Option<Vec<usize>>,
    filters: Vec<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
    count: usize,
}

impl Stream for FormatStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(limit) = self.limit {
            if self.count >= limit {
                return Poll::Ready(None);
            }
        }

        let batch = ready!(self.stream.as_mut().poll_next(cx));
        Poll::Ready(match batch {
            Some(Ok(batch)) => {
                // Apply filters
                let mut filtered_batch = batch;
                for filter in &self.filters {
                    let mask = filter
                        .evaluate(&filtered_batch)
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?
                        .into_array(filtered_batch.num_rows());
                    let mask_array = mask
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("Filter did not evaluate to boolean".to_string())
                        })?;
                    filtered_batch = filter_record_batch(&filtered_batch, mask_array)
                        .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                }

                // Apply projection if needed
                let projected_batch = if let Some(ref indices) = self.projection {
                    RecordBatch::try_new(
                        Arc::new(filtered_batch.schema().project(indices).unwrap()),
                        indices.iter().map(|&i| filtered_batch.column(i).clone()).collect(),
                    ).map_err(|e| DataFusionError::Internal(e.to_string()))?
                } else {
                    filtered_batch
                };

                self.count += 1;
                Some(Ok(projected_batch))
            }
            Some(Err(e)) => Some(Err(DataFusionError::Internal(e.to_string()))),
            None => None,
        })
    }
}

impl RecordBatchStream for FormatStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
