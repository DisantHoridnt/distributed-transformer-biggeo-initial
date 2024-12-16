use anyhow::Result;
use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use dotenv::dotenv;
use std::env;
use futures_util::TryStreamExt;
use tokio::io::AsyncReadExt;
use async_trait::async_trait;
use std::sync::Arc;
use parquet::file::metadata::ParquetMetaData;
use std::ops::Range;
use futures::future::BoxFuture;
use parquet::errors::ParquetError;

struct BytesReader {
    data: Bytes,
}

impl BytesReader {
    fn new(data: Bytes) -> Self {
        Self { data }
    }
}

#[async_trait]
impl AsyncFileReader for BytesReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        Box::pin(async move {
            Ok(self.data.slice(range))
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>, ParquetError>> {
        Box::pin(async move {
            let mut arrow_file = parquet::arrow::async_reader::AsyncFile::new(
                futures::io::Cursor::new(self.data.clone())
            );
            let metadata = arrow_file.metadata().await?;
            Ok(Arc::new(metadata))
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting the distributed transformer...");

    // Load environment variables from .env file
    dotenv().ok();

    // Initialize S3 connection using environment variables
    let s3 = AmazonS3Builder::new()
        .with_region(env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()))
        .with_bucket_name(env::var("S3_BUCKET_NAME").expect("S3_BUCKET_NAME must be set"))
        .build()?;

    // Read Parquet file from S3
    let path: Path = "input_data.parquet".into();
    let data: Bytes = s3.get(&path).await?.bytes().await?;

    // Create async reader
    let async_reader = BytesReader::new(data);
    let stream = ParquetRecordBatchStreamBuilder::new(async_reader)
        .await?
        .build()?;

    // Process record batches asynchronously
    let mut batch_count = 0;
    let mut batches = Vec::new();
    tokio::pin!(stream);
    while let Some(batch) = stream.try_next().await? {
        batch_count += 1;
        batches.push(batch);
    }

    println!("Successfully read {} record batches", batch_count);

    Ok(())
}