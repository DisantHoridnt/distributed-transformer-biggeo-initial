use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::stream::BoxStream;
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use object_store::GetResult;

mod parquet_format;
mod csv_format;
pub use parquet_format::ParquetFormat;
pub use csv_format::CsvFormat;

/// Represents a stream of data chunks from storage
pub type DataStream = BoxStream<'static, Result<Bytes>>;

#[async_trait]
pub trait SchemaInference {
    /// Infer schema from a data sample
    async fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef>;
    
    /// Validate that a batch conforms to the expected schema
    fn validate_schema(&self, schema: &SchemaRef, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != *schema {
            return Err(anyhow::anyhow!(
                "Schema mismatch. Expected: {:?}, Got: {:?}",
                schema,
                batch.schema()
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait DataFormat: Send + Sync + SchemaInference {
    /// Read batches from a stream of bytes
    async fn read_batches_from_stream(
        &self,
        schema: SchemaRef,
        stream: DataStream,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>;

    /// Read batches from a complete data buffer
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Create a single-item stream from the data
        let stream = futures::stream::once(async move { Ok(data) });
        
        // Infer schema
        let schema = self.infer_schema(&data).await?;
        
        // Use streaming implementation
        self.read_batches_from_stream(schema, Box::pin(stream)).await
    }

    /// Write batches to a byte buffer
    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes>;
    
    fn clone_box(&self) -> Box<dyn DataFormat + Send + Sync>;
}

type FormatFactory = fn() -> Box<dyn DataFormat + Send + Sync>;

lazy_static! {
    static ref FORMAT_REGISTRY: std::collections::HashMap<&'static str, FormatFactory> = {
        let mut m = std::collections::HashMap::new();
        m.insert("parquet", || Box::new(ParquetFormat));
        m.insert("csv", || Box::new(CsvFormat::new(true, 1024)));
        m
    };
}

pub fn get_format_for_extension(ext: &str) -> Option<Box<dyn DataFormat + Send + Sync>> {
    FORMAT_REGISTRY.get(ext).map(|factory| factory())
}
