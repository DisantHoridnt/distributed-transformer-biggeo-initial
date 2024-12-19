use anyhow::Result;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::dataframe::DataFrame;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

pub use csv_format::CsvFormat;
pub use parquet_format::ParquetFormat;

mod csv_format;
mod parquet_format;

pub trait DataFormat: Send + Sync {
    fn read(&self, data: &Bytes) -> Result<DataFrame>;
    fn write(&self, df: &DataFrame) -> Result<Bytes>;
    fn write_batch(&self, batch: &RecordBatch) -> Result<Bytes>;
}

pub struct FormatRegistry {
    formats: std::collections::HashMap<String, std::sync::Arc<Box<dyn DataFormat + Send + Sync>>>,
}

impl FormatRegistry {
    fn new() -> Self {
        let mut formats = std::collections::HashMap::new();
        formats.insert(
            "csv".to_string(),
            std::sync::Arc::new(Box::new(CsvFormat::default()) as Box<dyn DataFormat + Send + Sync>),
        );
        formats.insert(
            "parquet".to_string(),
            std::sync::Arc::new(Box::new(ParquetFormat::default()) as Box<dyn DataFormat + Send + Sync>),
        );
        Self { formats }
    }

    pub fn get_format(&self, format_name: &str) -> Option<std::sync::Arc<Box<dyn DataFormat + Send + Sync>>> {
        self.formats.get(format_name).cloned()
    }

    pub fn register_format(
        &mut self,
        format_name: String,
        format: Box<dyn DataFormat + Send + Sync>,
    ) {
        self.formats.insert(format_name, std::sync::Arc::new(format));
    }

    pub fn get_format_for_path(&self, path: &str) -> Option<std::sync::Arc<Box<dyn DataFormat + Send + Sync>>> {
        let extension = path.split('.').last()?;
        match extension {
            "csv" => Some(std::sync::Arc::new(Box::new(CsvFormat::default()) as Box<dyn DataFormat + Send + Sync>)),
            "parquet" => Some(std::sync::Arc::new(Box::new(ParquetFormat::default()) as Box<dyn DataFormat + Send + Sync>)),
            _ => None,
        }
    }
}

static FORMAT_REGISTRY: Lazy<RwLock<FormatRegistry>> = Lazy::new(|| RwLock::new(FormatRegistry::new()));

pub fn register_format(name: &str, format: Box<dyn DataFormat + Send + Sync>) {
    FORMAT_REGISTRY.write().register_format(name.to_string(), format);
}

pub fn get_format(name: &str) -> Option<std::sync::Arc<Box<dyn DataFormat + Send + Sync>>> {
    FORMAT_REGISTRY.read().get_format(name)
}

pub fn get_format_for_extension(extension: &str) -> Option<std::sync::Arc<Box<dyn DataFormat + Send + Sync>>> {
    match extension {
        "csv" => Some(std::sync::Arc::new(Box::new(CsvFormat::default()) as Box<dyn DataFormat + Send + Sync>)),
        "parquet" => Some(std::sync::Arc::new(Box::new(ParquetFormat::default()) as Box<dyn DataFormat + Send + Sync>)),
        _ => None,
    }
}
