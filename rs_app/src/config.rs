use std::path::PathBuf;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Global configuration for the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Format-specific configurations
    pub formats: FormatConfig,
    /// Plugin system configuration
    pub plugins: PluginConfig,
    /// Storage configuration
    pub storage: StorageConfig,
    /// Processing configuration
    pub processing: ProcessingConfig,
    /// Streaming configuration
    pub streaming: StreamingConfig,
}

/// Configuration for different data formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatConfig {
    /// CSV format configuration
    pub csv: CsvConfig,
    /// Parquet format configuration
    pub parquet: ParquetConfig,
    /// Default configuration for unknown formats
    pub default: DefaultFormatConfig,
}

/// CSV format specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvConfig {
    /// Default batch size for reading
    pub batch_size: usize,
    /// Whether to assume header row by default
    pub default_has_header: bool,
    /// Number of rows to sample for schema inference
    pub schema_sample_size: usize,
    /// Maximum sample size in bytes
    pub max_sample_bytes: usize,
    /// CSV delimiter
    pub delimiter: char,
    /// Quote character
    pub quote: char,
}

/// Parquet format specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    /// Default batch size for reading
    pub batch_size: usize,
    /// Whether to use statistics for optimization
    pub use_statistics: bool,
    /// Row group size for writing
    pub row_group_size: usize,
    /// Compression codec
    pub compression: String,
    /// Page size in bytes
    pub page_size: usize,
    /// Dictionary page size in bytes
    pub dictionary_page_size: usize,
}

/// Default configuration for unknown formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultFormatConfig {
    /// Default batch size
    pub batch_size: usize,
    /// Default sample size for schema inference
    pub schema_sample_size: usize,
}

/// Plugin system configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Directory to load plugins from
    pub plugin_dir: Option<PathBuf>,
    /// Whether to enable plugin loading
    pub enable_plugins: bool,
    /// Plugin-specific configurations
    pub plugin_configs: HashMap<String, serde_json::Value>,
    /// Plugin load timeout in seconds
    pub load_timeout_secs: u64,
    /// Enable hot reloading of plugins
    pub enable_hot_reload: bool,
    /// Hot reload check interval in seconds
    pub hot_reload_interval_secs: u64,
    /// Plugin version compatibility mode
    pub version_compatibility: VersionCompatibility,
}

/// Version compatibility modes for plugins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VersionCompatibility {
    /// Only load exact version matches
    Exact,
    /// Load compatible major versions
    Major,
    /// Load compatible major and minor versions
    Minor,
    /// Load any version (not recommended)
    Any,
}

/// Storage system configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Buffer size for streaming reads
    pub read_buffer_size: usize,
    /// Buffer size for streaming writes
    pub write_buffer_size: usize,
    /// Maximum concurrent requests
    pub max_concurrent_requests: usize,
    /// Retry configuration
    pub retry: RetryConfig,
}

/// Retry configuration for storage operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retries
    pub max_retries: u32,
    /// Initial retry delay in milliseconds
    pub initial_delay_ms: u64,
    /// Maximum retry delay in milliseconds
    pub max_delay_ms: u64,
    /// Retry backoff multiplier
    pub backoff_multiplier: f64,
}

/// Streaming configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    /// Maximum buffer memory per stream
    pub max_buffer_memory: usize,
    /// Number of buffers to pre-allocate
    pub buffer_pool_size: usize,
    /// Stream read timeout in seconds
    pub read_timeout_secs: u64,
    /// Stream write timeout in seconds
    pub write_timeout_secs: u64,
    /// Enable backpressure mechanisms
    pub enable_backpressure: bool,
    /// Maximum in-flight batches per stream
    pub max_in_flight_batches: usize,
}

/// Data processing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Maximum memory usage per operation
    pub max_memory_bytes: usize,
    /// Whether to use memory mapping
    pub use_memory_mapping: bool,
    /// Number of threads for parallel processing
    pub parallel_threads: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            formats: FormatConfig {
                csv: CsvConfig {
                    batch_size: 1024,
                    default_has_header: true,
                    schema_sample_size: 1000,
                    max_sample_bytes: 1024 * 1024, // 1MB
                    delimiter: ',',
                    quote: '"',
                },
                parquet: ParquetConfig {
                    batch_size: 1024,
                    use_statistics: true,
                    row_group_size: 128 * 1024 * 1024, // 128MB
                    compression: "snappy".to_string(),
                    page_size: 1024 * 1024, // 1MB
                    dictionary_page_size: 2 * 1024 * 1024, // 2MB
                },
                default: DefaultFormatConfig {
                    batch_size: 1024,
                    schema_sample_size: 1000,
                },
            },
            plugins: PluginConfig {
                plugin_dir: None,
                enable_plugins: false,
                plugin_configs: HashMap::new(),
                load_timeout_secs: 30,
                enable_hot_reload: false,
                hot_reload_interval_secs: 60,
                version_compatibility: VersionCompatibility::Major,
            },
            storage: StorageConfig {
                read_buffer_size: 8 * 1024 * 1024, // 8MB
                write_buffer_size: 8 * 1024 * 1024, // 8MB
                max_concurrent_requests: 10,
                retry: RetryConfig {
                    max_retries: 3,
                    initial_delay_ms: 100,
                    max_delay_ms: 5000,
                    backoff_multiplier: 2.0,
                },
            },
            streaming: StreamingConfig {
                max_buffer_memory: 256 * 1024 * 1024, // 256MB
                buffer_pool_size: 32,
                read_timeout_secs: 300,
                write_timeout_secs: 300,
                enable_backpressure: true,
                max_in_flight_batches: 4,
            },
            processing: ProcessingConfig {
                max_memory_bytes: 1024 * 1024 * 1024, // 1GB
                use_memory_mapping: true,
                parallel_threads: num_cpus::get(),
            },
        }
    }
}

impl Config {
    /// Load configuration from a file
    pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&contents)?)
    }

    /// Save configuration to a file
    pub fn save_to_file<P: AsRef<std::path::Path>>(&self, path: P) -> anyhow::Result<()> {
        let contents = serde_yaml::to_string(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }

    /// Get format-specific configuration
    pub fn get_format_config(&self, format: &str) -> &dyn FormatConfigTrait {
        match format {
            "csv" => &self.formats.csv,
            "parquet" => &self.formats.parquet,
            _ => &self.formats.default,
        }
    }

    /// Get plugin configuration
    pub fn get_plugin_config(&self, plugin_name: &str) -> Option<&serde_json::Value> {
        self.plugins.plugin_configs.get(plugin_name)
    }
}

/// Trait for format-specific configurations
pub trait FormatConfigTrait {
    fn batch_size(&self) -> usize;
    fn sample_size(&self) -> usize;
}

impl FormatConfigTrait for CsvConfig {
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn sample_size(&self) -> usize {
        self.schema_sample_size
    }
}

impl FormatConfigTrait for ParquetConfig {
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn sample_size(&self) -> usize {
        self.batch_size
    }
}

impl FormatConfigTrait for DefaultFormatConfig {
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    fn sample_size(&self) -> usize {
        self.schema_sample_size
    }
}
