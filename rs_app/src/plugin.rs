use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use libloading::Library;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::formats::DataFormat;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    pub description: String,
}

pub struct Plugin {
    pub metadata: PluginMetadata,
    pub library: Arc<Library>,
}

pub struct PluginRegistry {
    plugins: HashMap<String, Arc<Plugin>>,
    formats: HashMap<String, Arc<Box<dyn DataFormat + Send + Sync>>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
            formats: HashMap::new(),
        }
    }

    pub async fn register_format(&mut self, name: &str, format: Arc<Box<dyn DataFormat + Send + Sync>>) -> Result<()> {
        self.formats.insert(name.to_string(), format);
        Ok(())
    }

    pub fn get_format(&self, name: &str) -> Option<Arc<Box<dyn DataFormat + Send + Sync>>> {
        self.formats.get(name).cloned()
    }

    pub async fn load_plugin<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path = path.as_ref();
        let lib = unsafe { Library::new(path)? };
        
        let get_metadata: libloading::Symbol<unsafe fn() -> PluginMetadata> =
            unsafe { lib.get(b"get_metadata")? };
        
        let metadata = unsafe { get_metadata() };
        let plugin = Arc::new(Plugin {
            metadata: metadata.clone(),
            library: Arc::new(lib),
        });
        
        self.plugins.insert(metadata.name.clone(), plugin);
        Ok(())
    }
}

pub struct PluginManager {
    registry: Arc<RwLock<PluginRegistry>>,
    plugin_dir: PathBuf,
}

impl PluginManager {
    pub fn new<P: Into<PathBuf>>(plugin_dir: P) -> Self {
        Self {
            registry: Arc::new(RwLock::new(PluginRegistry::new())),
            plugin_dir: plugin_dir.into(),
        }
    }

    pub async fn load_plugins(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.plugin_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "so" || ext == "dylib") {
                let mut registry = self.registry.write();
                registry.load_plugin(&path).await?;
            }
        }
        
        Ok(())
    }

    pub async fn register_format(&self, name: &str, format: Arc<Box<dyn DataFormat + Send + Sync>>) -> Result<()> {
        self.registry.write().register_format(name, format).await
    }

    pub fn get_format(&self, name: &str) -> Option<Arc<Box<dyn DataFormat + Send + Sync>>> {
        self.registry.read().get_format(name)
    }
}

/// Trait that must be implemented by format plugins
pub trait FormatPlugin: Send + Sync {
    /// Create a new instance of the format
    fn create_format(&self) -> Box<dyn DataFormat + Send + Sync>;
    
    /// Get metadata about the plugin
    fn metadata(&self) -> &PluginMetadata;
}

/// Macro for plugin declaration
#[macro_export]
macro_rules! declare_plugin {
    ($plugin_type:ty, $create_fn:ident) => {
        #[no_mangle]
        pub extern "C" fn $create_fn() -> *mut dyn $crate::plugin::FormatPlugin {
            let plugin = <$plugin_type>::default();
            Box::into_raw(Box::new(plugin))
        }
    };
}
