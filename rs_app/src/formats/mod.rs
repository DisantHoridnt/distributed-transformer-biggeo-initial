use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;

mod parquet_format;
mod csv_format;
pub use parquet_format::ParquetFormat;
pub use csv_format::CsvFormat;

#[async_trait]
pub trait DataFormat: Send + Sync {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>>;
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
