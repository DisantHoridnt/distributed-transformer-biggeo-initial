pub mod config;
pub mod formats;
pub mod storage;
pub mod table_provider;
pub mod execution;
pub mod plugin;

// Re-export key traits and types
pub use config::Config;
pub use formats::{CsvFormat, DataFormat, ParquetFormat, SchemaInference};
pub use plugin::{FormatPlugin, PluginMetadata, PluginManager};
pub use storage::Storage;
pub use table_provider::FormatTableProvider;
