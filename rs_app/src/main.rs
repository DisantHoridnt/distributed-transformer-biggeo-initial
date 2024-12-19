use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use clap::{Parser, Subcommand};
use dotenv::dotenv;
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;
use url::Url;

use crate::formats::csv_format::{CsvConfig, CsvFormat};
use crate::formats::parquet_format::{ParquetConfig, ParquetFormat};
use crate::formats::DataFormat;
use crate::storage::azure::AzureStorage;
use crate::storage::local::LocalStorage;
use crate::storage::s3::S3Storage;
use crate::table_provider::FormatTableProvider;

mod formats;
mod storage;
mod table_provider;
mod execution;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Convert {
        #[arg(short, long)]
        input: String,
        #[arg(short, long)]
        output: String,
    },
}

async fn get_storage_for_url(url: &Url) -> Result<Box<dyn storage::Storage>> {
    match url.scheme() {
        "s3" => Ok(Box::new(S3Storage::new(url.host_str().unwrap().to_string())?)),
        "azure" => Ok(Box::new(AzureStorage::new(url.host_str().unwrap().to_string())?)),
        "file" | _ => Ok(Box::new(LocalStorage::new()?)),
    }
}

async fn get_format_for_url(url: &Url) -> Result<Box<dyn DataFormat + Send + Sync>> {
    let path = url.path();
    match path.split('.').last() {
        Some("csv") => Ok(Box::new(CsvFormat::new(&CsvConfig::default()))),
        Some("parquet") => Ok(Box::new(ParquetFormat::new(&ParquetConfig::default()))),
        _ => Err(anyhow::anyhow!("Unsupported file format")),
    }
}

async fn convert(input: &str, output: &str) -> Result<()> {
    // Parse URLs
    let input_url = Url::parse(input)?;
    let output_url = Url::parse(output)?;

    // Get storage implementations
    let input_storage = get_storage_for_url(&input_url).await?;
    let output_storage = get_storage_for_url(&output_url).await?;

    // Get format implementations
    let input_format = get_format_for_url(&input_url).await?;
    let output_format = get_format_for_url(&output_url).await?;

    // Read input data
    let input_data = input_storage.read(&input_url).await?;

    // Convert to record batches
    let mut input_stream = Box::pin(input_data.map(|result| {
        result.and_then(|bytes| {
            input_format
                .read_batch(&bytes)
                .map_err(|e| anyhow::anyhow!("Error reading input: {}", e))
        })
    }));

    // Get schema from first batch
    let first_batch = input_stream
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("No data in input"))??;
    let schema: SchemaRef = first_batch.schema();

    // Create a new stream that includes the first batch
    let input_stream = Box::pin(futures::stream::once(futures::future::ready(Ok(first_batch))));

    // Create table provider
    let provider = FormatTableProvider::new(input_format, schema.clone(), Box::pin(input_stream));

    // Execute the query
    let ctx = datafusion::execution::context::SessionContext::new();
    let df = ctx.read_table(Arc::new(provider))?;
    let batches = df.collect().await?;

    // Write output
    for batch in batches {
        let output_data = output_format.write_batch(&batch)?;
        output_storage.write(&output_url, output_data).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::Convert { input, output } => convert(&input, &output).await?,
    }

    Ok(())
}