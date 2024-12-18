use std::sync::Arc;
use anyhow::Result;
use arrow::array::RecordBatch;
use bytes::Bytes;
use clap::Parser;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use futures::StreamExt;
use std::io::Cursor;
use tokio;
use url::Url;

mod storage;
use storage::Storage;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    input_url: String,

    #[arg(short, long)]
    output_url: String,

    #[arg(short, long)]
    filter_sql: Option<String>,
}

async fn read_parquet_file(storage: Arc<dyn Storage>, url: &str) -> Result<Vec<RecordBatch>> {
    let data = storage.get(url).await?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(data, 1024)?;
    
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    
    Ok(batches)
}

async fn apply_sql_filter(batches: Vec<RecordBatch>, sql: &str) -> Result<Vec<RecordBatch>> {
    let schema = batches[0].schema();
    let mem_table = MemTable::try_new(schema, vec![batches])?;
    
    let ctx = SessionContext::new();
    ctx.register_table("data", Arc::new(mem_table))?;
    
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    
    Ok(results)
}

async fn write_parquet_file(storage: Arc<dyn Storage>, url: &str, batches: Vec<RecordBatch>) -> Result<()> {
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    {
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None)?;

        for batch in batches {
            writer.write(&batch)?;
        }

        writer.close()?;
    }
    
    storage.put(url, Bytes::from(buf)).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    
    let args = Args::parse();
    
    let input_url = Url::parse(&args.input_url)?;
    let output_url = Url::parse(&args.output_url)?;
    
    let storage = storage::from_url(&input_url).await?;
    
    // Read input file
    let mut batches = read_parquet_file(storage.clone(), &args.input_url).await?;
    
    // Apply SQL filter if provided
    if let Some(sql) = args.filter_sql {
        batches = apply_sql_filter(batches, &sql).await?;
    }
    
    // Write output file
    write_parquet_file(storage, &output_url.path().trim_start_matches('/'), batches).await?;
    
    Ok(())
}