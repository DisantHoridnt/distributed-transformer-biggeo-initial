use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures::{StreamExt, TryStreamExt};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::{ObjectStore, path::Path as ObjectPath};
use url::Url;

pub struct AzureStorage {
    store: Box<dyn ObjectStore>,
    container: String,
}

impl AzureStorage {
    pub fn new(container: String) -> Result<Self> {
        let store = MicrosoftAzureBuilder::new()
            .with_container_name(&container)
            .build()?;
        Ok(Self {
            store: Box::new(store),
            container,
        })
    }

    fn get_object_path(&self, url: &Url) -> Result<ObjectPath> {
        let path = url.path();
        Ok(ObjectPath::from(path))
    }
}

#[async_trait]
impl super::Storage for AzureStorage {
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
        let stream = futures::stream::once(async move { result.bytes().await })
            .map_err(anyhow::Error::from)
            .map_ok(|bytes| bytes);
        Ok(Box::new(stream))
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
