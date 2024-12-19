use std::path::PathBuf;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use num_cpus;

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
    pub has_header: bool,
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
    /// Default compression type (uncompressed, snappy, gzip, brotli, zstd)
    pub compression: String,
    /// Number of rows to sample for schema inference
    pub schema_sample_size: usize,
    /// Maximum sample size in bytes
    pub max_sample_bytes: usize,
}

/// Default configuration for unknown formats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultFormatConfig {
    /// Default batch size for reading
    pub batch_size: usize,
    /// Number of rows to sample for schema inference
    pub schema_sample_size: usize,
    /// Maximum sample size in bytes
    pub max_sample_bytes: usize,
}

/// Plugin system configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Directory containing plugins
    pub directory: PathBuf,
    /// Plugin version compatibility mode
    pub version_compatibility: VersionCompatibility,
    /// Whether to load plugins in isolated processes
    pub isolated_loading: bool,
    /// Maximum number of concurrent plugin instances
    pub max_instances: usize,
    /// Plugin-specific configuration
    pub plugin_configs: HashMap<String, serde_json::Value>,
    /// Default timeout for plugin operations in seconds
    pub default_timeout: u64,
}

/// Version compatibility modes for plugins
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VersionCompatibility {
    /// Only load exact version matches
    Exact,
    /// Load plugins with compatible major version
    Major,
    /// Load plugins with compatible major and minor version
    Minor,
    /// Load any version (not recommended)
    Any,
}

/// Storage system configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// S3 configuration
    pub s3: S3Config,
    /// Local storage configuration
    pub local_path: PathBuf,
    /// Retry configuration
    pub retry: RetryConfig,
}

/// S3 configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// AWS region
    pub region: String,
    /// S3 endpoint URL (optional)
    pub endpoint: Option<String>,
    /// Default bucket
    pub bucket: String,
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
}

/// Streaming configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    /// Maximum number of concurrent streams
    pub max_concurrent_streams: usize,
    /// Stream buffer size in bytes
    pub buffer_size: usize,
    /// Stream timeout in seconds
    pub timeout: u64,
    /// Whether to use compression for streaming
    pub use_compression: bool,
}

/// Data processing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Number of worker threads
    pub num_threads: usize,
    /// Memory limit per thread in bytes
    pub memory_limit: usize,
}

impl Default for Config {
    fn default() -> Self {
        let num_cpus = num_cpus::get();
        Self {
            formats: FormatConfig {
                csv: CsvConfig {
                    batch_size: 1024,
                    has_header: true,
                    schema_sample_size: 1000,
                    max_sample_bytes: 1024 * 1024,
                    delimiter: ',',
                    quote: '"',
                },
                parquet: ParquetConfig {
                    batch_size: 1024,
                    compression: "snappy".to_string(),
                    schema_sample_size: 1000,
                    max_sample_bytes: 1024 * 1024,
                },
                default: DefaultFormatConfig {
                    batch_size: 1024,
                    schema_sample_size: 1000,
                    max_sample_bytes: 1024 * 1024,
                },
            },
            plugins: PluginConfig {
                directory: PathBuf::from("plugins"),
                version_compatibility: VersionCompatibility::Minor,
                isolated_loading: true,
                max_instances: num_cpus,
                plugin_configs: HashMap::new(),
                default_timeout: 30,
            },
            storage: StorageConfig {
                s3: S3Config {
                    region: "us-east-1".to_string(),
                    endpoint: None,
                    bucket: "default".to_string(),
                },
                local_path: PathBuf::from("data"),
                retry: RetryConfig {
                    max_retries: 3,
                    initial_delay_ms: 100,
                    max_delay_ms: 5000,
                },
            },
            processing: ProcessingConfig {
                num_threads: num_cpus,
                memory_limit: 1024 * 1024 * 1024,
            },
            streaming: StreamingConfig {
                max_concurrent_streams: num_cpus * 2,
                buffer_size: 1024 * 1024,
                timeout: 30,
                use_compression: true,
            },
        }
    }
}

impl Config {
    /// Load configuration from a file
    pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }

    /// Save configuration to a file
    pub fn save_to_file<P: AsRef<std::path::Path>>(&self, path: P) -> anyhow::Result<()> {
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(path, content)?;
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
        self.schema_sample_size
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
