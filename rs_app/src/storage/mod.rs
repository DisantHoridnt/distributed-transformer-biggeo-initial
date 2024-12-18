use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use url::Url;

mod azure;
mod local;
mod s3;

pub use azure::AzureStorage;
pub use local::LocalStorage;
pub use s3::S3Storage;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn get(&self, path: &str) -> Result<Bytes>;
    async fn put(&self, path: &str, data: Bytes) -> Result<()>;
}

pub fn from_url(url: &Url) -> Result<Arc<dyn Storage>> {
    match url.scheme() {
        "file" => {
            let path = url.path();
            Ok(Arc::new(LocalStorage::new(path)?))
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
        scheme => Err(anyhow!("Unsupported storage scheme: {}", scheme)),
    }
}
