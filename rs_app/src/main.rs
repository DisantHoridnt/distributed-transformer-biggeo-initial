use anyhow::Result;
use clap::{Parser, Subcommand};
use dotenv::dotenv;
use url::Url;

use crate::formats::{CsvFormat, DataFormat, ParquetFormat};
use crate::storage::azure::AzureStorage;
use crate::storage::local::LocalStorage;
use crate::storage::s3::S3Storage;

mod formats;
mod storage;
mod table_provider;
mod execution;

use datafusion::prelude::*;

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
        #[arg(long)]
        filter_sql: Option<String>,
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
        Some("csv") => Ok(Box::new(CsvFormat::default())),
        Some("parquet") => Ok(Box::new(ParquetFormat::default())),
        _ => Err(anyhow::anyhow!("Unsupported file format")),
    }
}

async fn convert(input: &str, output: &str, filter_sql: Option<String>) -> Result<()> {
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
    let input_data = input_storage.read_all(&input_url).await?;
    let mut df = input_format.read(&input_data)?;

    // Apply filter if provided
    if let Some(sql) = filter_sql {
        let ctx = SessionContext::new();
        ctx.register_table("data", df.clone().into_view())?;
        let sql = if sql.to_lowercase().contains("where") {
            format!("SELECT * FROM data WHERE {} LIMIT 10", sql)
        } else {
            format!("SELECT * FROM data {} LIMIT 10", sql)
        };
        df = ctx.sql(&sql).await?;
    }

    // Write output
    let output_data = output_format.write(&df)?;
    output_storage.write(&output_url, output_data).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let cli = Cli::parse();

    match cli.command {
        Commands::Convert { input, output, filter_sql } => convert(&input, &output, filter_sql).await?,
    }

    Ok(())
}