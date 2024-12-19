use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::common::ToDFSchema;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;

use crate::execution::FormatExecPlan;
use crate::formats::DataFormat;

pub struct FormatTableProvider {
    format: Arc<Box<dyn DataFormat + Send + Sync>>,
    schema: SchemaRef,
    data: Pin<Box<dyn Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + Sync + 'static>>,
}

impl FormatTableProvider {
    pub fn new(
        format: Arc<Box<dyn DataFormat + Send + Sync>>,
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Convert logical expressions to physical expressions
        let df_schema = self.schema.as_ref().to_dfschema()?;
        let physical_filters = filters
            .iter()
            .filter_map(|expr| {
                create_physical_expr(
                    expr,
                    self.schema.as_ref(),
                    self.schema.as_ref(),
                    state.execution_props(),
                )
                .ok()
                .map(|expr| Arc::new(expr) as Arc<dyn PhysicalExpr>)
            })
            .collect::<Vec<_>>();

        // Create a stream that yields the data
        let stream = Box::pin(futures::stream::once(futures::future::ready(Ok(RecordBatch::new_empty(self.schema.clone())))));

        let exec = FormatExecPlan::new(
            stream,
            self.schema.clone(),
            projection.cloned(),
            physical_filters,
            limit,
        );
        Ok(Arc::new(exec))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}
