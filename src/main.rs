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
use parquet::file::reader::{ChunkReader, Length, FileReader};
use parquet::file::serialized_reader::SerializedFileReader;
use std::io::{self, Cursor, Read, Seek, SeekFrom};

struct BytesReader {
    data: Bytes,
}

impl BytesReader {
    fn new(data: Bytes) -> Self {
        Self { data }
    }
}

struct SyncReader {
    data: Bytes,
    pos: usize,
}

impl SyncReader {
    fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }
}

impl Length for SyncReader {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl Read for SyncReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let available = self.data.len() - self.pos;
        let amount = buf.len().min(available);
        if amount > 0 {
            buf[..amount].copy_from_slice(&self.data[self.pos..self.pos + amount]);
            self.pos += amount;
        }
        Ok(amount)
    }
}

impl Seek for SyncReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.data.len() as i64 + offset,
            SeekFrom::Current(offset) => self.pos as i64 + offset,
        };

        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Seek before start of file",
            ));
        }

        self.pos = new_pos as usize;
        Ok(self.pos as u64)
    }
}

impl ChunkReader for SyncReader {
    type T = Self;

    fn get_bytes(&self, range: Range<usize>) -> Result<Bytes, ParquetError> {
        Ok(self.data.slice(range))
    }

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        let mut reader = SyncReader::new(self.data.clone());
        reader.seek(SeekFrom::Start(start))?;
        Ok(reader)
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
        let data = self.data.clone();
        Box::pin(async move {
            let reader = SyncReader::new(data);
            let file_reader = SerializedFileReader::new(reader)?;
            let metadata = file_reader.metadata();
            Ok(Arc::new(metadata.clone()))
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