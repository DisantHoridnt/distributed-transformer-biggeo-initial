use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use url::Url;

pub mod azure;
pub mod local;
pub mod s3;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn read(&self, url: &Url) -> Result<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send + Sync + Unpin + 'static>>;
    async fn read_all(&self, url: &Url) -> Result<Bytes>;
    async fn write(&self, url: &Url, data: Bytes) -> Result<()>;
}

pub fn from_url(url: &Url) -> Result<Box<dyn Storage>> {
    match url.scheme() {
        "file" => {
            let storage = local::LocalStorage::new()?;
            Ok(Box::new(storage))
        }
        "s3" => {
            let storage = s3::S3Storage::new(url.host_str().unwrap_or("").to_string())?;
            Ok(Box::new(storage))
        }
        "azure" => {
            let storage = azure::AzureStorage::new(url.host_str().unwrap_or("").to_string())?;
            Ok(Box::new(storage))
        }
        _ => Err(anyhow::anyhow!("Unsupported URL scheme")),
    }
}
