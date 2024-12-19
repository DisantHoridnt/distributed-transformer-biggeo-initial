use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::stream::BoxStream;
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use object_store::GetResult;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

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

// Format registry that combines built-in and plugin formats
static FORMAT_REGISTRY: Lazy<RwLock<std::collections::HashMap<String, Box<dyn Fn() -> Box<dyn DataFormat + Send + Sync> + Send + Sync>>>> = 
    Lazy::new(|| {
        let mut m = std::collections::HashMap::new();
        // Register built-in formats
        m.insert("parquet".to_string(), Box::new(|| Box::new(ParquetFormat::default())));
        m.insert("csv".to_string(), Box::new(|| Box::new(CsvFormat::new(true, 1024))));
        RwLock::new(m)
    });

/// Get a format implementation by name
pub fn get_format(name: &str) -> Option<Box<dyn DataFormat + Send + Sync>> {
    FORMAT_REGISTRY.read().get(name).map(|factory| factory())
}

/// Register a new format implementation
pub fn register_format<F>(name: &str, factory: F)
where
    F: Fn() -> Box<dyn DataFormat + Send + Sync> + Send + Sync + 'static,
{
    FORMAT_REGISTRY.write().insert(name.to_string(), Box::new(factory));
}

/// Get a format implementation for a file extension
pub fn get_format_for_extension(extension: &str) -> Option<Box<dyn DataFormat + Send + Sync>> {
    // First check plugins
    if let Some(plugin) = crate::plugin::PluginManager::get_plugin_for_extension(extension) {
        return Some(plugin.create_format());
    }
    
    // Then check built-in formats
    match extension {
        "parquet" => Some(Box::new(ParquetFormat::default())),
        "csv" => Some(Box::new(CsvFormat::new(true, 1024))),
        _ => None,
    }
}
