use super::Storage;
use async_trait::async_trait;
use bytes::Bytes;
use anyhow::Result;
use futures::stream::{self, BoxStream};
use std::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use std::path::PathBuf;

pub struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    pub fn new(base_path: String) -> Self {
        Self { base_path: PathBuf::from(base_path) }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn list(&self, prefix: Option<&str>) -> Result<BoxStream<'static, Result<String>>> {
        let entries = fs::read_dir(&self.base_path)?;
        let keys = entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path()
                    .strip_prefix(&self.base_path)
                    .ok()?
                    .to_string_lossy()
                    .into_owned();
                if let Some(p) = prefix {
                    if path.contains(p) {
                        Some(Ok(path))
                    } else {
                        None
                    }
                } else {
                    Some(Ok(path))
                }
            })
            .collect::<Vec<_>>();

        Ok(stream::iter(keys).boxed())
    }

    async fn get(&self, path: &str) -> Result<Bytes> {
        let full_path = self.base_path.join(path);
        let mut file = File::open(full_path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        Ok(Bytes::from(buf))
    }

    async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let full_path = self.base_path.join(path);
        // Ensure parent directory exists
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(full_path, &data).await?;
        Ok(())
    }
}
