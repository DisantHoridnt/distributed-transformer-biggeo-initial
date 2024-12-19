use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::Stream;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

pub mod csv_format;
pub mod parquet_format;

pub use csv_format::CsvFormat;
pub use parquet_format::ParquetFormat;

pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;
pub type DataStream = BoxStream<'static, Result<Bytes>>;

#[async_trait::async_trait]
pub trait SchemaInference: Send + Sync {
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

#[async_trait::async_trait]
pub trait DataFormat: Send + Sync + SchemaInference {
    /// Read batches from a stream of bytes
    async fn read_batches_from_stream(
        &self,
        schema: SchemaRef,
        stream: DataStream,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>>;

    /// Read batches from a complete data buffer
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // First infer schema
        let schema = self.infer_schema(&data).await?;
        
        // Create a single-item stream from the data
        let stream = futures::stream::once(async move { Ok(data) });
        
        // Use streaming implementation
        self.read_batches_from_stream(schema, Box::pin(stream)).await
    }

    /// Write batches to a byte buffer
    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes>;
}

pub struct FormatRegistry {
    formats: HashMap<String, Arc<Box<dyn DataFormat + Send + Sync>>>,
}

impl FormatRegistry {
    pub fn new() -> Self {
        let mut formats = HashMap::new();
        formats.insert("csv".to_string(), Arc::new(Box::new(CsvFormat::default()) as Box<dyn DataFormat + Send + Sync>));
        formats.insert("parquet".to_string(), Arc::new(Box::new(ParquetFormat::default()) as Box<dyn DataFormat + Send + Sync>));
        Self { formats }
    }

    pub fn register_format(&mut self, name: &str, format: Box<dyn DataFormat + Send + Sync>) {
        self.formats.insert(name.to_string(), Arc::new(format));
    }

    pub fn get_format(&self, name: &str) -> Option<Arc<Box<dyn DataFormat + Send + Sync>>> {
        self.formats.get(name).cloned()
    }
}

static FORMAT_REGISTRY: Lazy<RwLock<FormatRegistry>> = Lazy::new(|| {
    RwLock::new(FormatRegistry::new())
});

pub fn register_format(name: &str, format: Box<dyn DataFormat + Send + Sync>) {
    FORMAT_REGISTRY.write().register_format(name, format);
}

pub fn get_format(name: &str) -> Option<Arc<Box<dyn DataFormat + Send + Sync>>> {
    FORMAT_REGISTRY.read().get_format(name)
}

/// Get a format implementation for a file extension
pub fn get_format_for_extension(extension: &str) -> Option<Arc<Box<dyn DataFormat + Send + Sync>>> {
    match extension {
        "csv" => Some(Arc::new(Box::new(CsvFormat::default()) as Box<dyn DataFormat + Send + Sync>)),
        "parquet" => Some(Arc::new(Box::new(ParquetFormat::default()) as Box<dyn DataFormat + Send + Sync>)),
        _ => None,
    }
}
