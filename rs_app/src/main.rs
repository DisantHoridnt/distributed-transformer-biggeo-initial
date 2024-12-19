use std::sync::Arc;
use anyhow::Result;
use clap::Parser;
use datafusion::prelude::*;
use futures::StreamExt;
use url::Url;

mod config;
mod formats;
mod storage;
mod table_provider;

use config::Config;
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

    /// Parquet compression (uncompressed, snappy, gzip, brotli, zstd)
    #[arg(long, default_value = "snappy")]
    compression: String,

    /// Config file path
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    let mut config = Config::default();
    if let Some(config_path) = args.config {
        config = Config::from_file(config_path)?;
    }

    // Update config with command line arguments
    if let Some(batch_size) = args.batch_size {
        config.formats.csv.batch_size = batch_size;
        config.formats.parquet.batch_size = batch_size;
    }

    if let Some(has_header) = args.has_header {
        config.formats.csv.has_header = has_header;
    }

    let input_url = Url::parse(&args.input_url)?;
    let output_url = Url::parse(&args.output_url)?;

    let input_format = if args.input_format == "auto" {
        match input_url.path().split('.').last() {
            Some("csv") => "csv",
            Some("parquet") => "parquet",
            _ => return Err(anyhow::anyhow!("Could not determine input format")),
        }
    } else {
        &args.input_format
    };

    let output_format = if args.output_format == "auto" {
        match output_url.path().split('.').last() {
            Some("csv") => "csv",
            Some("parquet") => "parquet",
            _ => return Err(anyhow::anyhow!("Could not determine output format")),
        }
    } else {
        &args.output_format
    };

    let input_format: Box<dyn DataFormat + Send + Sync> = match input_format {
        "csv" => Box::new(CsvFormat::new(&config.formats.csv)),
        "parquet" => Box::new(ParquetFormat::new(&config.formats.parquet)),
        _ => return Err(anyhow::anyhow!("Unsupported input format")),
    };

    let output_format: Box<dyn DataFormat + Send + Sync> = match output_format {
        "csv" => Box::new(CsvFormat::new(&config.formats.csv)),
        "parquet" => Box::new(ParquetFormat::new(&config.formats.parquet)),
        _ => return Err(anyhow::anyhow!("Unsupported output format")),
    };

    let storage = Arc::new(storage::from_url(&input_url)?);
    let input_stream = storage.read(&input_url).await?;
    let schema = input_format.infer_schema(&storage.read_all(&input_url).await?).await?;

    let provider = FormatTableProvider::new(input_format, schema.clone(), input_stream);
    let ctx = SessionContext::new();
    ctx.register_table("input", Arc::new(provider))?;

    let df = ctx.table("input").await?;
    let batches = df.collect().await?;

    let output_storage = Arc::new(storage::from_url(&output_url)?);
    let output_stream = futures::stream::iter(batches.into_iter().map(Ok));
    let output_bytes = output_format.write_batches(Box::pin(output_stream)).await?;
    output_storage.write(&output_url, output_bytes).await?;

    Ok(())
}