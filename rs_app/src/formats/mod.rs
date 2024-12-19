use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use anyhow::Result;
use bytes::Bytes;
use std::collections::HashMap;
use lazy_static::lazy_static;

mod parquet_format;
pub use parquet_format::ParquetFormat;

#[async_trait]
pub trait DataFormat: Send + Sync {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>>;
    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes>;
}

type FormatFactory = fn() -> Box<dyn DataFormat + Send + Sync>;

lazy_static! {
    static ref FORMAT_REGISTRY: HashMap<&'static str, FormatFactory> = {
        let mut m = HashMap::new();
        m.insert("parquet", || Box::new(ParquetFormat));
        m
    };
}

pub fn get_format_for_extension(ext: &str) -> Option<Box<dyn DataFormat + Send + Sync>> {
    FORMAT_REGISTRY.get(ext).map(|factory| factory())
}
