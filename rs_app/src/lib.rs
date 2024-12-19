pub mod formats;
pub mod storage;
pub mod execution;
pub mod table_provider;
pub mod plugin;

// Re-export key traits and types
pub use formats::{DataFormat, SchemaInference};
pub use plugin::{FormatPlugin, PluginMetadata, PluginManager};
