use std::sync::Arc;
use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use clap::Parser;
use datafusion::prelude::*;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio;
use url::Url;

mod config;
mod formats;
mod storage;
mod table_provider;

use config::{Config, CsvConfig, ParquetConfig};
use formats::{CsvFormat, DataFormat, ParquetFormat};
use storage::Storage;
use table_provider::FormatTableProvider;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input URL (e.g., file:///path/to/input.csv, s3://bucket/input.csv)
    #[arg(short, long)]
    input_url: String,

    /// Output URL (e.g., file:///path/to/output.parquet, s3://bucket/output.parquet)
    #[arg(short, long)]
    output_url: String,

    /// Input format (csv or parquet)
    #[arg(long, default_value = "auto")]
    input_format: String,

    /// Output format (csv or parquet)
    #[arg(long, default_value = "auto")]
    output_format: String,

    /// Batch size for processing
    #[arg(long)]
    batch_size: Option<usize>,

    /// CSV has header
    #[arg(long)]
    has_header: Option<bool>,

    /// CSV delimiter
    #[arg(long, default_value = ",")]
    delimiter: String,

    /// Parquet compression (snappy, gzip, brotli, lz4, zstd)
    #[arg(long, default_value = "snappy")]
    compression: String,

    /// Parquet row group size
    #[arg(long)]
    row_group_size: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Create configuration
    let mut config = Config::new();
    if let Some(batch_size) = args.batch_size {
        config.csv.batch_size = batch_size;
        config.parquet.batch_size = batch_size;
    }
    if let Some(has_header) = args.has_header {
        config.csv.has_header = has_header;
    }
    config.csv.delimiter = args.delimiter.chars().next().unwrap_or(',');
    config.parquet.compression = args.compression;
    if let Some(row_group_size) = args.row_group_size {
        config.parquet.row_group_size = row_group_size;
    }

    // Parse URLs
    let input_url = Url::parse(&args.input_url)?;
    let output_url = Url::parse(&args.output_url)?;

    // Detect formats from file extensions if not specified
    let input_format = if args.input_format == "auto" {
        match input_url.path().split('.').last() {
            Some("csv") => "csv",
            Some("parquet") => "parquet",
            _ => return Err(anyhow::anyhow!("Could not detect input format from file extension")),
        }
    } else {
        &args.input_format
    };

    let output_format = if args.output_format == "auto" {
        match output_url.path().split('.').last() {
            Some("csv") => "csv",
            Some("parquet") => "parquet",
            _ => return Err(anyhow::anyhow!("Could not detect output format from file extension")),
        }
    } else {
        &args.output_format
    };

    // Create format instances
    let input_format: Box<dyn DataFormat> = match input_format {
        "csv" => Box::new(CsvFormat::new(&config.csv)),
        "parquet" => Box::new(ParquetFormat::new(&config.parquet)),
        _ => anyhow::bail!("Unsupported input format: {}", input_format),
    };

    let output_format: Box<dyn DataFormat> = match output_format {
        "csv" => Box::new(CsvFormat::new(&config.csv)),
        "parquet" => Box::new(ParquetFormat::new(&config.parquet)),
        _ => anyhow::bail!("Unsupported output format: {}", output_format),
    };

    // Create storage instances
    let input_storage = storage::from_url(&input_url).await?;
    let output_storage = storage::from_url(&output_url).await?;

    // Read input data
    let input_path = input_url.path();
    let input_data = input_storage.get(input_path).await?;
    let mut batches = input_format.read_batches(input_data).await?;

    // Process and write output
    let mut output_data = Vec::new();
    while let Some(batch) = batches.try_next().await? {
        output_data.push(batch);
    }

    let output_bytes = output_format
        .write_batches(Box::pin(futures::stream::iter(output_data.into_iter().map(Ok))))
        .await?;
    
    // Write output data
    let output_path = output_url.path();
    output_storage.put(output_path, output_bytes).await?;

    Ok(())
}