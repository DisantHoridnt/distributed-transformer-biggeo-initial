# Distributed Transformer

A high-performance, streaming ETL system built in Rust using DataFusion, with support for multiple data formats and dynamic plugin loading.

## Features

### Core Processing
- **Streaming Processing**: Zero-copy streaming for large datasets
- **Memory Efficient**: Buffer pool with backpressure mechanisms
- **Format Agnostic**: Extensible format system with built-in CSV and Parquet support
- **Plugin System**: Dynamic format discovery and loading

### Data Formats
- **CSV**
  - Streaming read/write
  - Schema inference
  - Configurable delimiters and quotes
  - Header detection

- **Parquet**
  - Row group-based streaming
  - Multiple compression codecs
  - Statistics optimization
  - Dictionary encoding

### Performance Features
- **Buffer Pool**
  - Pre-allocated memory management
  - RAII-based buffer handling
  - Configurable pool sizes
  - Automatic buffer recycling

- **Streaming Optimizations**
  - Backpressure control
  - Configurable batch sizes
  - Memory-mapped I/O support
  - Parallel processing

### Plugin System
- **Dynamic Loading**: Runtime format discovery
- **Version Control**: Compatibility checking
- **Hot Reload**: Optional runtime updates
- **Custom Configs**: Per-plugin configuration

## Configuration

The system uses a comprehensive configuration system:
- YAML-based configuration files
- Type-safe configuration structs
- Extensive validation
- Default values for all settings

See [Configuration Documentation](docs/configuration.md) for details.

## Getting Started

1. **Installation**
   ```bash
   cargo build --release
   ```

2. **Basic Usage**
   ```rust
   use distributed_transformer::{Config, CsvFormat, ParquetFormat};

   // Load configuration
   let config = Config::from_file("config.yaml")?;

   // Create format with configuration
   let format = CsvFormat::new(&config.formats.csv);

   // Process data
   let provider = FormatTableProvider::try_new(format, storage, "data.csv").await?;
   ```

3. **Configuration**
   Copy and modify the example configuration:
   ```bash
   cp config.example.yaml config.yaml
   ```

## Documentation

- [Configuration Guide](docs/configuration.md)
- [Migration Guide](docs/migration.md)
- [Plugin Development](docs/plugins.md)

## Technical Details

### Memory Management
The system uses a sophisticated memory management system:
```rust
pub struct BufferPool {
    buffers: Arc<Mutex<Vec<BytesMut>>>,
    semaphore: Arc<Semaphore>,
    buffer_size: usize,
    max_memory: usize,
}
```

### Streaming Architecture
```rust
pub trait DataFormat: Send + Sync {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>>;
    async fn read_batches_from_stream(&self, schema: SchemaRef, stream: DataStream) 
        -> Result<BoxStream<'static, Result<RecordBatch>>>;
    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes>;
}
```

### Plugin System
```rust
pub trait FormatPlugin: Send + Sync {
    fn metadata(&self) -> &PluginMetadata;
    fn create_format(&self, config: &Config) -> Result<Box<dyn DataFormat + Send + Sync>>;
}
```

## Performance Considerations

### Memory Usage
- Configure `max_buffer_memory` based on available RAM
- Adjust `buffer_pool_size` for workload
- Set appropriate batch sizes

### Throughput
- Tune `max_in_flight_batches`
- Configure parallel processing threads
- Optimize row group sizes for Parquet

### Storage
- Configure buffer sizes for I/O
- Set appropriate retry policies
- Limit concurrent requests

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.