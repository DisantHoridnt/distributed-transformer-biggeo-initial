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
use futures::StreamExt;

use crate::formats::DataFormat;
use crate::storage::Storage;
use crate::execution::FormatExecPlan;

pub struct FormatTableProvider {
    format: Box<dyn DataFormat + Send + Sync>,
    storage: Arc<dyn Storage>,
    data: Option<Bytes>,
    schema: SchemaRef,
}

impl FormatTableProvider {
    pub async fn try_new(
        format: Box<dyn DataFormat + Send + Sync>,
        storage: Arc<dyn Storage>,
        path: &str,
    ) -> Result<Self> {
        // Read a small sample to infer the schema
        let data = storage.get(path).await?;
        let mut stream = format.read_batches(data.clone()).await?;
        
        // Get the first batch to extract schema
        let first_batch = stream
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("No data in source"))??;
            
        let schema = first_batch.schema();
        
        Ok(Self {
            format,
            storage,
            data: Some(data),
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
        if let Some(data) = &self.data {
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

            // Create execution plan with streaming support
            Ok(Arc::new(FormatExecPlan::new(
                self.format.clone_box(),
                data.clone(),
                self.schema(),
                projection.cloned(),
                physical_filters,
            )))
        } else {
            Err(DataFusionError::Internal("No data available".to_string()))
        }
    }
}
