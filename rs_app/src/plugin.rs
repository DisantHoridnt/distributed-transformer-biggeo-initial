use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;
use libloading::{Library, Symbol};
use once_cell::sync::Lazy;
use parking_lot::RwLock;

use crate::formats::DataFormat;

/// Plugin metadata containing information about a format plugin
#[derive(Debug)]
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    pub extensions: Vec<String>,
    pub description: String,
}

/// Trait that must be implemented by format plugins
#[async_trait]
pub trait FormatPlugin: Send + Sync {
    /// Get metadata about the plugin
    fn metadata(&self) -> &PluginMetadata;
    
    /// Create a new instance of the format
    fn create_format(&self) -> Box<dyn DataFormat + Send + Sync>;
}

type PluginRegistry = HashMap<String, Arc<dyn FormatPlugin>>;

/// Global plugin registry
static PLUGIN_REGISTRY: Lazy<RwLock<PluginRegistry>> = Lazy::new(|| RwLock::new(HashMap::new()));

/// Plugin manager for loading and managing format plugins
pub struct PluginManager {
    plugin_dir: PathBuf,
    loaded_libraries: Vec<Library>,
}

impl PluginManager {
    pub fn new<P: Into<PathBuf>>(plugin_dir: P) -> Self {
        Self {
            plugin_dir: plugin_dir.into(),
            loaded_libraries: Vec::new(),
        }
    }
    
    /// Load all plugins from the plugin directory
    pub fn load_plugins(&mut self) -> Result<()> {
        let entries = std::fs::read_dir(&self.plugin_dir)?;
        
        for entry in entries {
            let path = entry?.path();
            if path.extension().map_or(false, |ext| {
                ext == std::env::consts::DLL_EXTENSION
            }) {
                self.load_plugin(&path)?;
            }
        }
        
        Ok(())
    }
    
    /// Load a single plugin from a dynamic library
    fn load_plugin(&mut self, path: &std::path::Path) -> Result<()> {
        unsafe {
            let library = Library::new(path)?;
            
            // Get plugin creation function
            let create_plugin: Symbol<unsafe extern "C" fn() -> *mut dyn FormatPlugin> = 
                library.get(b"create_plugin")?;
                
            // Create plugin instance
            let plugin = Arc::new(create_plugin());
            
            // Register plugin
            let metadata = plugin.metadata();
            PLUGIN_REGISTRY.write().insert(metadata.name.clone(), plugin);
            
            // Keep library loaded
            self.loaded_libraries.push(library);
        }
        
        Ok(())
    }
    
    /// Get a format plugin by name
    pub fn get_plugin(name: &str) -> Option<Arc<dyn FormatPlugin>> {
        PLUGIN_REGISTRY.read().get(name).cloned()
    }
    
    /// Get a format plugin by file extension
    pub fn get_plugin_for_extension(extension: &str) -> Option<Arc<dyn FormatPlugin>> {
        PLUGIN_REGISTRY.read().values().find(|plugin| {
            plugin.metadata().extensions.iter().any(|ext| ext == extension)
        }).cloned()
    }
    
    /// List all loaded plugins
    pub fn list_plugins() -> Vec<PluginMetadata> {
        PLUGIN_REGISTRY.read()
            .values()
            .map(|plugin| plugin.metadata().clone())
            .collect()
    }
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
