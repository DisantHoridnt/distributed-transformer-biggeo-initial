# Configuration System Documentation

## Overview
The distributed-transformer configuration system provides a comprehensive, type-safe way to configure all aspects of the data processing pipeline. It uses YAML for configuration files and provides Rust structs for programmatic access.

## Core Components

### Format Configuration
```rust
pub struct FormatConfig {
    pub csv: CsvConfig,
    pub parquet: ParquetConfig,
    pub default: DefaultFormatConfig,
}
```

#### CSV Format
- `batch_size`: Number of rows per batch (default: 1024)
- `default_has_header`: Header row detection (default: true)
- `schema_sample_size`: Rows to sample for schema inference (default: 1000)
- `max_sample_bytes`: Maximum bytes to read for sampling (default: 1MB)
- `delimiter`: CSV delimiter character (default: ',')
- `quote`: Quote character (default: '"')

#### Parquet Format
- `batch_size`: Number of rows per batch (default: 1024)
- `use_statistics`: Enable statistics for optimization (default: true)
- `row_group_size`: Size of row groups (default: 128MB)
- `compression`: Compression codec (default: "snappy")
- `page_size`: Page size in bytes (default: 1MB)
- `dictionary_page_size`: Dictionary page size (default: 2MB)

### Streaming Configuration
```rust
pub struct StreamingConfig {
    pub max_buffer_memory: usize,    // 256MB default
    pub buffer_pool_size: usize,     // 32 default
    pub read_timeout_secs: u64,      // 300s default
    pub write_timeout_secs: u64,     // 300s default
    pub enable_backpressure: bool,   // true default
    pub max_in_flight_batches: usize // 4 default
}
```

### Plugin System
```rust
pub struct PluginConfig {
    pub plugin_dir: Option<PathBuf>,
    pub enable_plugins: bool,
    pub plugin_configs: HashMap<String, serde_json::Value>,
    pub load_timeout_secs: u64,
    pub enable_hot_reload: bool,
    pub hot_reload_interval_secs: u64,
    pub version_compatibility: VersionCompatibility,
}
```

## Memory Management

### Buffer Pool
The system uses a memory-efficient buffer pool for streaming operations:
- Pre-allocated buffers for reduced allocation overhead
- RAII-based buffer management
- Backpressure mechanisms for memory safety
- Configurable pool size and buffer sizes

Example:
```rust
let pool = BufferPool::new(&config.streaming);
let buffer = pool.acquire().await?;
// Buffer is automatically returned to pool when dropped
```

### Batch Processing
- Configurable batch sizes per format
- Memory limits for processing operations
- Streaming support for large datasets
- Row group optimization for Parquet

## Performance Considerations

### Memory Usage
- Buffer pool size affects memory footprint
- Row group size impacts Parquet read/write performance
- Batch size influences processing efficiency

### Throughput
- Configurable number of in-flight batches
- Backpressure prevents memory exhaustion
- Parallel processing thread configuration

### Storage
- Configurable read/write buffer sizes
- Retry mechanisms with exponential backoff
- Concurrent request limiting

## Validation

The configuration system includes comprehensive validation:
```rust
pub fn validate_config(config: &Config) -> Result<()>
```

Validates:
- Memory limits and buffer sizes
- Format-specific settings
- Plugin configuration
- Streaming parameters
- Processing options

## Example Configuration
See [config.example.yaml](../config.example.yaml) for a complete example with comments.
