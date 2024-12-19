use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{ObjectStore, path::Path as ObjectPath};
use url::Url;

pub struct S3Storage {
    store: Box<dyn ObjectStore>,
    bucket: String,
}

impl S3Storage {
    pub fn new(bucket: String) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(&bucket)
            .with_allow_http(true)
            .with_region(std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "us-east-1".to_string()))
            .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID")?)
            .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY")?)
            .with_endpoint("https://s3.us-east-1.amazonaws.com")
            .with_skip_signature(true)
            .build()?;
        Ok(Self {
            store: Box::new(store),
            bucket,
        })
    }

    fn get_object_path(&self, url: &Url) -> Result<ObjectPath> {
        let path = url.path();
        Ok(ObjectPath::from(path))
    }
}

#[async_trait]
impl super::Storage for S3Storage {
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let prefix = prefix.unwrap_or("");
        let path = ObjectPath::from(prefix);
        let mut entries = Vec::new();
        let mut stream = self.store.list(Some(&path));
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            entries.push(entry.location.to_string());
        }
        Ok(entries)
    }

    async fn read(&self, url: &Url) -> Result<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Send + Sync + Unpin + 'static>> {
        let path = self.get_object_path(url)?;
        let result = self.store.get(&path).await?;
        let bytes = result.bytes().await?;
        let stream = futures::stream::once(futures::future::ready(Ok(bytes)));
        Ok(Box::new(Box::pin(stream)))
    }

    async fn read_all(&self, url: &Url) -> Result<Bytes> {
        let path = self.get_object_path(url)?;
        let data = self.store.get(&path).await?.bytes().await?;
        Ok(data)
    }

    async fn write(&self, url: &Url, data: Bytes) -> Result<()> {
        let path = self.get_object_path(url)?;
        self.store.put(&path, data.into()).await?;
        Ok(())
    }
}
