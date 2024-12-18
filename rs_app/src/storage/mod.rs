use std::sync::Arc;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use url::Url;

pub mod azure;
pub mod local;
pub mod s3;

use azure::AzureStorage;
use local::LocalStorage;
use s3::S3Storage;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn get(&self, path: &str) -> Result<Bytes>;
    async fn put(&self, path: &str, data: Bytes) -> Result<()>;
}

pub async fn from_url(url: &Url) -> Result<Arc<dyn Storage>> {
    match url.scheme() {
        "file" => {
            let path = url.path().to_string();
            let storage = LocalStorage::new(path.into()).await?;
            Ok(Arc::new(storage))
        }
        "s3" => {
            let bucket = url.host_str()
                .ok_or_else(|| anyhow!("Missing bucket name in S3 URL"))?
                .to_string();
            Ok(Arc::new(S3Storage::new(bucket)?))
        }
        "azure" => {
            let container = url.host_str()
                .ok_or_else(|| anyhow!("Missing container name in Azure URL"))?
                .to_string();
            Ok(Arc::new(AzureStorage::new(container)?))
        }
        _ => Err(anyhow!("Unsupported URL scheme: {}", url.scheme())),
    }
}
