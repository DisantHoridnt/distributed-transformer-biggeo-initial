use std::sync::Arc;
use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::util::pretty::print_batches;
use bytes::Bytes;
use clap::Parser;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use futures::StreamExt;
use std::io::Cursor;
use tokio;
use url::Url;

mod storage;
mod formats;
use storage::Storage;
use formats::{DataFormat, ParquetFormat};

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

async fn read_file(storage: Arc<dyn Storage>, url: &Url) -> Result<Vec<RecordBatch>> {
    let path = url.path();
    let data = storage.get(path).await?;
    
    // Get the format based on file extension
    let ext = url.path()
        .split('.')
        .last()
        .ok_or_else(|| anyhow::anyhow!("No file extension found"))?;
        
    let format = formats::get_format_for_extension(ext)
        .ok_or_else(|| anyhow::anyhow!("Unsupported file format: {}", ext))?;
    
    let stream = format.read_batches(data).await?;
    let batches = stream.try_collect().await?;
    
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

async fn write_file(storage: Arc<dyn Storage>, url: &Url, batches: Vec<RecordBatch>) -> Result<()> {
    let ext = url.path()
        .split('.')
        .last()
        .ok_or_else(|| anyhow::anyhow!("No file extension found"))?;
        
    let format = formats::get_format_for_extension(ext)
        .ok_or_else(|| anyhow::anyhow!("Unsupported file format: {}", ext))?;
    
    let stream = futures::stream::iter(batches.into_iter().map(Ok));
    let data = format.write_batches(Box::pin(stream)).await?;
    
    let path = url.path();
    storage.put(path, data).await?;
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
    let mut batches = read_file(storage.clone(), &input_url).await?;
    
    println!("\nInput data sample:");
    print_batches(&batches)?;
    
    // Apply SQL filter if provided
    if let Some(sql) = args.filter_sql {
        println!("\nApplying SQL filter: {}", sql);
        batches = apply_sql_filter(batches, &sql).await?;
        
        println!("\nFiltered data:");
        print_batches(&batches)?;
    }
    
    // Write output file
    write_file(storage, &output_url, batches).await?;
    println!("\nOutput written to: {}", output_url);
    
    Ok(())
}