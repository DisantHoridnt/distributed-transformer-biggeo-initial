use super::Storage;
use async_trait::async_trait;
use bytes::Bytes;
use anyhow::Result;
use futures::stream::BoxStream;
use object_store::{path::Path, ObjectStore};

pub struct AzureStorage {
    store: Box<dyn ObjectStore>,
}

impl AzureStorage {
    pub fn new(store: Box<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Storage for AzureStorage {
    async fn list(&self, prefix: Option<&str>) -> Result<BoxStream<'static, Result<String>>> {
        let prefix_path = prefix.map(Path::from);
        let stream = self.store.list(prefix_path).await?;
        let mapped = stream
            .map_ok(|meta| meta.location.to_string())
            .boxed();
        Ok(mapped)
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        let path = Path::from(path);
        let data = self.store.get(&path).await?.bytes().await?;
        Ok(data)
    }

    async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let path = Path::from(path);
        self.store.put(&path, data).await?;
        Ok(())
    }
}
