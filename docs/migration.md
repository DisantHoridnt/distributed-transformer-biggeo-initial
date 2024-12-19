# Migration Guide for Configuration Changes

## Overview
This document outlines the changes made to the configuration system and provides guidance for migrating existing code to use the new configuration structure.

## Major Changes

### 1. Configuration File
- New YAML-based configuration system
- Centralized configuration for all components
- Type-safe configuration structs
- Default values provided for all settings

### 2. Format-Specific Changes

#### CSV Format
```rust
// Old
let format = CsvFormat::new(true, 1024);

// New
let config = Config::default();
let format = CsvFormat::new(&config.formats.csv);
```

#### Parquet Format
```rust
// Old
let format = ParquetFormat::new(1024);

// New
let config = Config::default();
let format = ParquetFormat::new(&config.formats.parquet);
```

### 3. Streaming Changes
- New buffer pool for efficient memory management
- Configurable backpressure mechanisms
- Timeout settings for read/write operations

```rust
// Old
let stream = format.read_batches_from_stream(schema, input_stream);

// New
let config = Config::default();
let pool = BufferPool::new(&config.streaming);
let stream = format.read_batches_from_stream(schema, input_stream);
```

### 4. Plugin System
- Version compatibility controls
- Hot reload support
- Per-plugin configuration

```rust
// Old
let plugin_manager = PluginManager::new("./plugins");

// New
let config = Config::default();
let plugin_manager = PluginManager::new(&config.plugins);
```

## Migration Steps

1. **Create Configuration File**
   - Copy `config.example.yaml` to your project
   - Modify settings as needed
   - Load configuration in your application:
   ```rust
   let config = Config::from_file("config.yaml")?;
   ```

2. **Update Format Usage**
   - Replace direct format instantiation with config-based creation
   - Update any custom format implementations to use the new configuration

3. **Enable Streaming Features**
   - Initialize the buffer pool
   - Configure streaming settings in config.yaml
   - Update streaming code to use the new buffer pool

4. **Plugin System Updates**
   - Update plugin directory structure if using hot reload
   - Add version information to plugins
   - Configure plugin-specific settings in config.yaml

## Breaking Changes

1. **Format Constructors**
   - All format constructors now require configuration
   - Direct parameter passing is no longer supported

2. **Streaming API**
   - Buffer management is now handled by BufferPool
   - New timeout and backpressure mechanisms

3. **Plugin System**
   - Stricter version compatibility checking
   - New plugin metadata requirements

## Example Configuration

See `config.example.yaml` for a complete example configuration with comments.

## Validation

The new configuration system includes validation for:
- Memory limits
- Buffer sizes
- Timeouts
- Plugin compatibility

## Performance Considerations

1. **Memory Usage**
   - Configure `max_buffer_memory` based on available system memory
   - Adjust `buffer_pool_size` for your workload

2. **Streaming Performance**
   - Tune `max_in_flight_batches` for throughput
   - Configure backpressure settings for stability

3. **Format-Specific**
   - Adjust batch sizes for optimal performance
   - Configure compression settings for Parquet

## Support

If you encounter issues during migration:
1. Check the example configuration
2. Verify all required settings are present
3. Review the validation errors
4. Open an issue with details about your configuration
