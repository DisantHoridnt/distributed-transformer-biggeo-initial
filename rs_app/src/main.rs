use std::sync::Arc;
use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::util::pretty::print_batches;
use bytes::Bytes;
use clap::Parser;
use datafusion::prelude::*;
use futures::StreamExt;
use tokio;
use url::Url;

mod storage;
mod formats;
mod table_provider;
use storage::Storage;
use formats::{DataFormat, ParquetFormat};
use table_provider::FormatTableProvider;

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

async fn process_data(
    storage: Arc<dyn Storage>,
    input_url: &Url,
    output_url: &Url,
    sql: Option<String>
) -> Result<()> {
    let path = input_url.path();
    let ext = path
        .split('.')
        .last()
        .ok_or_else(|| anyhow::anyhow!("No file extension found"))?;
        
    let format = formats::get_format_for_extension(ext)
        .ok_or_else(|| anyhow::anyhow!("Unsupported file format: {}", ext))?;
    
    // Create table provider
    let provider = FormatTableProvider::try_new(format, storage.clone(), path).await?;
    
    // Create session context and register table
    let ctx = SessionContext::new();
    ctx.register_table("data", Arc::new(provider))?;
    
    // Execute query
    let df = match sql {
        Some(sql) => ctx.sql(&sql).await?,
        None => ctx.table("data").await?,
    };
    
    // Collect results
    let results = df.collect().await?;
    println!("\nProcessed data:");
    print_batches(&results)?;
    
    // Write output
    let ext = output_url.path()
        .split('.')
        .last()
        .ok_or_else(|| anyhow::anyhow!("No file extension found"))?;
        
    let format = formats::get_format_for_extension(ext)
        .ok_or_else(|| anyhow::anyhow!("Unsupported file format: {}", ext))?;
    
    let stream = futures::stream::iter(results.into_iter().map(Ok));
    let data = format.write_batches(Box::pin(stream)).await?;
    
    storage.put(output_url.path(), data).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    
    let args = Args::parse();
    
    let input_url = Url::parse(&args.input_url)?;
    let output_url = Url::parse(&args.output_url)?;
    
    let storage = storage::from_url(&input_url).await?;
    
    process_data(storage, &input_url, &output_url, args.filter_sql).await?;
    
    println!("\nOutput written to: {}", output_url);
    
    Ok(())
}