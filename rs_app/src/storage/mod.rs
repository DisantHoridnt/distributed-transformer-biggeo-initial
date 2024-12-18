use async_trait::async_trait;
use bytes::Bytes;
use anyhow::{anyhow, Context, Result};
use futures::stream::BoxStream;

mod local;
mod s3;
mod azure;
#[cfg(test)]
mod tests;

pub use local::LocalStorage;
pub use s3::S3Storage;
pub use azure::AzureStorage;

#[async_trait]
pub trait Storage: Send + Sync {
    /// List objects with an optional prefix
    async fn list(&self, prefix: Option<&str>) -> Result<BoxStream<'static, Result<String>>>;
    
    /// Get object data by path
    async fn get(&self, path: &str) -> Result<Bytes>;
    
    /// Put object data at path
    async fn put(&self, path: &str, data: Bytes) -> Result<()>;
}

/// Create a storage backend from a URL string
pub async fn from_url(url: &str) -> Result<Box<dyn Storage>> {
    let parsed = url::Url::parse(url)
        .with_context(|| format!("Failed to parse URL: {}", url))?;

    let scheme = parsed.scheme();
    match scheme {
        "file" => {
            let base_path = parsed.path().trim_start_matches('/').to_string();
            Ok(Box::new(LocalStorage::new(base_path)))
        }
        "s3" => {
            let bucket = parsed.host_str()
                .ok_or_else(|| anyhow!("No bucket specified in S3 URL: {}", url))?;
            let region = std::env::var("AWS_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string());
            let access_key = std::env::var("AWS_ACCESS_KEY_ID")
                .context("AWS_ACCESS_KEY_ID environment variable not set")?;
            let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
                .context("AWS_SECRET_ACCESS_KEY environment variable not set")?;

            let store = object_store::aws::AmazonS3Builder::new()
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key)
                .with_region(&region)
                .with_bucket_name(bucket)
                .build()
                .context("Failed to create S3 client")?;
            
            Ok(Box::new(S3Storage::new(Box::new(store))))
        }
        "azure" => {
            let container = parsed.host_str()
                .ok_or_else(|| anyhow!("No container specified in Azure URL: {}", url))?;
            let account = std::env::var("AZURE_STORAGE_ACCOUNT")
                .context("AZURE_STORAGE_ACCOUNT environment variable not set")?;
            let access_key = std::env::var("AZURE_STORAGE_ACCESS_KEY")
                .context("AZURE_STORAGE_ACCESS_KEY environment variable not set")?;

            let store = object_store::azure::MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_access_key(access_key)
                .with_container_name(container)
                .build()
                .context("Failed to create Azure client")?;
            
            Ok(Box::new(AzureStorage::new(Box::new(store))))
        }
        _ => Err(anyhow!("Unsupported storage scheme: {}", scheme)),
    }
}
