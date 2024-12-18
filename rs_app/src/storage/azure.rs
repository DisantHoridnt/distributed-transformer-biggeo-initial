use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::{path::Path, ObjectStore};

use super::Storage;

pub struct AzureStorage {
    store: Box<dyn ObjectStore>,
    container: String,
}

impl AzureStorage {
    pub fn new(container: String) -> Result<Self> {
        let store = MicrosoftAzureBuilder::from_env()
            .with_container_name(&container)
            .build()?;
        
        Ok(Self {
            store: Box::new(store),
            container,
        })
    }
}

#[async_trait]
impl Storage for AzureStorage {
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
        let path = Path::from(path);
        let data = self.store.get(&path).await?.bytes().await?;
        Ok(data)
    }

    async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let path = Path::from(path);
        self.store.put(&path, data.into()).await?;
        Ok(())
    }
}
