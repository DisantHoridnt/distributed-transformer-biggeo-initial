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

pub use csv_format::{CsvConfig, CsvFormat};
pub use parquet_format::{ParquetConfig, ParquetFormat};

pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;
pub type DataStream = BoxStream<'static, Result<Bytes>>;

pub trait DataFormat: Send + Sync {
    /// Read a batch of data from bytes
    fn read_batch(&self, data: &Bytes) -> Result<RecordBatch>;

    /// Write a batch of data to bytes
    fn write_batch(&self, batch: &RecordBatch) -> Result<Bytes>;

    /// Infer schema from data
    fn infer_schema(&self, data: &Bytes) -> Result<SchemaRef>;
}

pub struct FormatRegistry {
    formats: HashMap<String, Arc<Box<dyn DataFormat + Send + Sync>>>,
}

impl FormatRegistry {
    pub fn new() -> Self {
        let mut formats = HashMap::new();
        formats.insert(
            "csv".to_string(),
            Arc::new(Box::new(CsvFormat::default()) as Box<dyn DataFormat + Send + Sync>),
        );
        formats.insert(
            "parquet".to_string(),
            Arc::new(Box::new(ParquetFormat::default()) as Box<dyn DataFormat + Send + Sync>),
        );
        Self { formats }
    }

    pub fn get_format(&self, format_name: &str) -> Option<Arc<Box<dyn DataFormat + Send + Sync>>> {
        self.formats.get(format_name).cloned()
    }

    pub fn register_format(
        &mut self,
        format_name: String,
        format: Box<dyn DataFormat + Send + Sync>,
    ) {
        self.formats.insert(format_name, Arc::new(format));
    }

    pub fn get_format_for_path(&self, path: &str) -> Option<Arc<Box<dyn DataFormat + Send + Sync>>> {
        let extension = path.split('.').last()?;
        match extension {
            "csv" => Some(Arc::new(Box::new(CsvFormat::default()) as Box<dyn DataFormat + Send + Sync>)),
            "parquet" => Some(Arc::new(Box::new(ParquetFormat::default()) as Box<dyn DataFormat + Send + Sync>)),
            _ => None,
        }
    }
}

static FORMAT_REGISTRY: Lazy<RwLock<FormatRegistry>> = Lazy::new(|| {
    RwLock::new(FormatRegistry::new())
});

pub fn register_format(name: &str, format: Box<dyn DataFormat + Send + Sync>) {
    FORMAT_REGISTRY.write().register_format(name.to_string(), format);
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
