use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::{path::Path, ObjectStore};

use super::Storage;

pub struct S3Storage {
    store: Box<dyn ObjectStore>,
    bucket: String,
}

impl S3Storage {
    pub fn new(bucket: String) -> Result<Self> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .with_allow_http(true)  // Allow non-HTTPS connections
            .build()?;
        
        Ok(Self {
            store: Box::new(store),
            bucket,
        })
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let prefix_path = prefix.map(Path::from);
        let mut stream = self.store.list(prefix_path.as_ref());
        
        let mut entries = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            entries.push(meta.location.to_string());
        }
        
        Ok(entries)
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        let path = Path::from(path.trim_start_matches('/'));  // Remove leading slash
        let data = self.store.get(&path).await?.bytes().await?;
        Ok(data)
    }

    async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let path = Path::from(path.trim_start_matches('/'));  // Remove leading slash
        self.store.put(&path, data.into()).await?;
        Ok(())
    }
}
