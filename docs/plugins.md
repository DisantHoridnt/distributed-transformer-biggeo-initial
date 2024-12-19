# Plugin Development Guide

## Overview
The distributed-transformer plugin system allows for dynamic loading of new data formats at runtime. This guide explains how to create, test, and deploy plugins.

## Plugin Architecture

### Core Components

#### Plugin Trait
```rust
pub trait FormatPlugin: Send + Sync {
    fn metadata(&self) -> &PluginMetadata;
    fn create_format(&self, config: &Config) -> Result<Box<dyn DataFormat + Send + Sync>>;
}
```

#### Plugin Metadata
```rust
pub struct PluginMetadata {
    pub name: String,
    pub version: String,
    pub description: String,
    pub extensions: Vec<String>,
    pub capabilities: PluginCapabilities,
}
```

#### Format Interface
```rust
pub trait DataFormat: Send + Sync {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>>;
    async fn read_batches_from_stream(&self, schema: SchemaRef, stream: DataStream) 
        -> Result<BoxStream<'static, Result<RecordBatch>>>;
    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes>;
}
```

## Creating a Plugin

### 1. Project Structure
```
my_plugin/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── format.rs
│   └── plugin.rs
└── tests/
    └── integration_tests.rs
```

### 2. Cargo.toml Configuration
```toml
[package]
name = "my_plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
distributed_transformer = { path = "../.." }
arrow = "47.0"
async-trait = "0.1"
bytes = "1.0"
futures = "0.3"
```

### 3. Implementation Example

#### lib.rs
```rust
mod format;
mod plugin;

use distributed_transformer::plugin::{FormatPlugin, PluginMetadata};

#[no_mangle]
pub extern "C" fn create_plugin() -> Box<dyn FormatPlugin> {
    Box::new(plugin::MyPlugin::new())
}
```

#### format.rs
```rust
use async_trait::async_trait;
use distributed_transformer::formats::DataFormat;

pub struct MyFormat {
    config: Arc<MyFormatConfig>,
}

#[async_trait]
impl DataFormat for MyFormat {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Implementation
    }

    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes> {
        // Implementation
    }
}
```

## Plugin Configuration

### 1. Format-Specific Config
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MyFormatConfig {
    pub batch_size: usize,
    pub custom_setting: String,
}
```

### 2. Plugin Registration
```rust
impl FormatPlugin for MyPlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn create_format(&self, config: &Config) -> Result<Box<dyn DataFormat + Send + Sync>> {
        let format_config: MyFormatConfig = config
            .plugins
            .plugin_configs
            .get("my_plugin")
            .ok_or_else(|| anyhow!("Missing plugin config"))?
            .try_into()?;

        Ok(Box::new(MyFormat::new(&format_config)))
    }
}
```

## Testing

### 1. Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_read_batches() {
        let config = MyFormatConfig::default();
        let format = MyFormat::new(&config);
        
        // Test implementation
    }
}
```

### 2. Integration Tests
```rust
use distributed_transformer::plugin::PluginManager;

#[tokio::test]
async fn test_plugin_loading() {
    let manager = PluginManager::new(&config.plugins)?;
    let plugin = manager.load_plugin("my_plugin.so")?;
    
    assert_eq!(plugin.metadata().name, "my_plugin");
}
```

## Deployment

### 1. Building
```bash
cargo build --release
```

### 2. Installation
```bash
cp target/release/libmy_plugin.so /path/to/plugins/
```

### 3. Configuration
```yaml
plugins:
  plugin_dir: "/path/to/plugins"
  enable_plugins: true
  plugin_configs:
    my_plugin:
      batch_size: 1024
      custom_setting: "value"
```

## Best Practices

1. **Memory Management**
   - Use buffer pool for streaming operations
   - Implement proper cleanup in Drop traits
   - Handle resource limits properly

2. **Error Handling**
   - Provide detailed error messages
   - Implement proper error propagation
   - Handle all potential failure cases

3. **Performance**
   - Implement streaming properly
   - Use appropriate batch sizes
   - Consider memory usage patterns

4. **Testing**
   - Write comprehensive unit tests
   - Include integration tests
   - Test with various data sizes

5. **Documentation**
   - Document public APIs
   - Include usage examples
   - Document configuration options
