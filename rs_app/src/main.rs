use anyhow::Result;
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
use clap::Parser;
use datafusion::prelude::*;
use url::Url;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

mod storage;
use storage::Storage;

#[derive(Parser)]
struct Config {
    #[clap(long)]
    input_url: String,
    #[clap(long)]
    output_url: String,
    #[clap(long)]
    filter_sql: Option<String>,
}

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

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        let start = start as usize;
        let end = start.checked_add(length).ok_or_else(|| {
            ParquetError::General("Integer overflow when calculating end index".to_string())
        })?;

        if end > self.data.len() {
            return Err(ParquetError::EOF(
                "Requested range extends beyond data length".to_string(),
            ));
        }

        Ok(self.data.slice(start..end))
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
        let data = self.data.clone();
        Box::pin(async move {
            if range.end > data.len() {
                return Err(ParquetError::EOF("Requested range extends beyond data length".to_string()));
            }
            Ok(data.slice(range))
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

async fn apply_sql_filter(batches: Vec<RecordBatch>, sql: &str) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    
    if batches.is_empty() {
        return Ok(vec![]);
    }

    let schema = batches[0].schema();
    let mem_table = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("data", Arc::new(mem_table))?;

    let df = ctx.sql(sql).await?;
    let result = df.collect().await?;
    Ok(result)
}

async fn write_parquet(storage: &dyn Storage, path: &str, batches: &[RecordBatch]) -> Result<()> {
    let mut buf = Vec::new();
    {
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(&mut buf, batches[0].schema(), Some(props))?;
        
        for batch in batches {
            writer.write(batch)?;
        }
        writer.close()?;
    }
    
    storage.put(path, Bytes::from(buf)).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let config = Config::parse();

    // Initialize storage backends
    let input_storage = storage::from_url(&config.input_url).await?;
    let output_storage = storage::from_url(&config.output_url).await?;

    // Extract input path from URL
    let input_url = Url::parse(&config.input_url)?;
    let input_path = input_url.path().trim_start_matches('/');
    
    // Extract output path from URL
    let output_url = Url::parse(&config.output_url)?;
    let output_path = output_url.path().trim_start_matches('/');

    // Read input data
    let input_data = input_storage.get(input_path).await?;
    let reader = BytesReader::new(input_data);
    
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .build()?;
    
    let mut batches: Vec<RecordBatch> = stream.try_collect().await?;

    // Apply SQL filter if provided
    if let Some(sql) = config.filter_sql {
        batches = apply_sql_filter(batches, &sql)?;
    }

    // Write output
    write_parquet(&*output_storage, output_path, &batches).await?;

    Ok(())
}