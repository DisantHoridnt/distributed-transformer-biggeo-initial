use anyhow::Result;
use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use dotenv::dotenv;
use std::env;
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting the distributed transformer...");

    // Load environment variables from .env file
    dotenv().ok();

    // Initialize S3 connection using environment variables
    let s3 = AmazonS3Builder::new()
        .with_region(env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string()))
        .with_bucket_name(env::var("S3_BUCKET_NAME").expect("S3_BUCKET_NAME must be set"))
        .build()
        .await?;

    // Read Parquet file from S3
    let path: Path = "input_data.parquet".into();
    let data = s3.get(&path).await?.bytes().await?;

    // Convert to Arrow RecordBatches
    let cursor = Cursor::new(data);
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)?;
    let mut reader = builder.build()?;

    // Read all batches
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch?);
    }

    println!("Successfully read {} record batches", batches.len());

    Ok(())
}