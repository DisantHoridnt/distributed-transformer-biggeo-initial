use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::{path::Path, ObjectStore};
use std::path::PathBuf;
use tokio::fs;

use super::Storage;

pub struct LocalStorage {
    store: Box<dyn ObjectStore>,
    base_path: PathBuf,
}

impl LocalStorage {
    pub async fn new(base_path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&base_path).await?;
        let store = LocalFileSystem::new_with_prefix(&base_path)?;
        
        Ok(Self {
            store: Box::new(store),
            base_path,
        })
    }
}

#[async_trait]
impl Storage for LocalStorage {
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
