use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_expr::PhysicalExpr;
use futures::{StreamExt, Stream};
use tokio::sync::Mutex;

use crate::formats::{DataFormat, DataStream};
use crate::storage::Storage;
use crate::execution::FormatExecPlan;

pub struct FormatTableProvider {
    format: Box<dyn DataFormat + Send + Sync>,
    storage: Arc<dyn Storage>,
    path: String,
    schema: SchemaRef,
}

impl FormatTableProvider {
    pub async fn try_new(
        format: Box<dyn DataFormat + Send + Sync>,
        storage: Arc<dyn Storage>,
        path: &str,
    ) -> Result<Self> {
        // Get a small sample to infer schema
        let mut stream = storage.get_stream(path).await?;
        let mut sample = Vec::new();
        let mut sample_size = 0;
        const MAX_SAMPLE_SIZE: usize = 1024 * 1024; // 1MB sample size
        
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            sample.extend_from_slice(&chunk);
            sample_size += chunk.len();
            if sample_size >= MAX_SAMPLE_SIZE {
                break;
            }
        }
        
        // Infer schema from sample
        let schema = format.infer_schema(&sample).await?;
        
        Ok(Self {
            format,
            storage,
            path: path.to_string(),
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for FormatTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Convert logical filters to physical expressions
        let physical_filters: Vec<Arc<dyn PhysicalExpr>> = filters
            .iter()
            .map(|expr| {
                expr.to_physical_expr(
                    state.create_physical_expr_context(),
                    self.schema(),
                    &state.config().options(),
                )
            })
            .collect::<datafusion::error::Result<_>>()?;

        // Get streaming data from storage
        let stream = self.storage
            .get_stream(&self.path)
            .await
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;

        // Create execution plan with streaming support
        Ok(Arc::new(FormatExecPlan::new(
            self.format.clone_box(),
            stream,
            self.schema(),
            projection.cloned(),
            physical_filters,
        )))
    }
}
