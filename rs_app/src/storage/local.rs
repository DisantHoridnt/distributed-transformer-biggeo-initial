use std::path::{Path, PathBuf};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, StreamExt};
use tokio::fs;

use super::Storage;

pub struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;
        Ok(Self { base_path })
    }

    fn resolve_path(&self, path: &str) -> PathBuf {
        self.base_path.join(path.trim_start_matches('/'))
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let base_dir = match prefix {
            Some(prefix) => self.resolve_path(prefix),
            None => self.base_path.clone(),
        };

        let mut entries = Vec::new();
        let mut read_dir = fs::read_dir(&base_dir).await?;
        
        while let Some(entry) = read_dir.next_await {
            if let Ok(entry) = entry {
                if let Ok(file_type) = entry.file_type().await {
                    if file_type.is_file() {
                        if let Ok(path) = entry.path().strip_prefix(&self.base_path) {
                            if let Some(path_str) = path.to_str() {
                                entries.push(path_str.to_string());
                            }
                        }
                    }
                }
            }
        }
        
        Ok(entries)
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        let file_path = self.resolve_path(path);
        let data = fs::read(file_path).await?;
        Ok(Bytes::from(data))
    }

    async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let file_path = self.resolve_path(path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(file_path, data).await?;
        Ok(())
    }
}
