# Data Format Handling

## Overview

The Distributed Transformer supports multiple data formats through a pluggable architecture. Currently, it supports CSV to Parquet conversion, with the ability to extend to other formats.

## Current Format Support

### CSV Format
- Input format parsing using Arrow's CSV reader
- Schema inference from CSV headers
- Configurable options:
  - Delimiter
  - Has header
  - Column types

### Parquet Format
- Output format writing using Arrow's Parquet writer
- Schema preservation
- Compression options
- Row group size configuration

## Adding New Formats

### Step 1: Implement the DataFormat Trait

Create a new file in `src/formats/` (e.g., `json_format.rs`) and implement the `DataFormat` trait:

```rust
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::dataframe::DataFrame;

pub struct JsonFormat {
    config: JsonConfig,
}

impl DataFormat for JsonFormat {
    fn read(&self, data: &Bytes) -> Result<DataFrame> {
        // 1. Parse the input bytes as JSON
        // 2. Convert to Arrow RecordBatch
        // 3. Create DataFrame
    }

    fn write(&self, df: &DataFrame) -> Result<Bytes> {
        // 1. Convert DataFrame to desired format
        // 2. Return as bytes
    }
}
```

### Step 2: Add Format Configuration

```rust
#[derive(Debug, Clone)]
pub struct JsonConfig {
    pub array_encoding: JsonArrayEncoding,
    pub compression: Option<CompressionType>,
}

impl JsonConfig {
    pub fn new() -> Self {
        Self {
            array_encoding: JsonArrayEncoding::List,
            compression: None,
        }
    }
}
```

### Step 3: Register Format in Registry

In `src/formats/mod.rs`:

```rust
lazy_static! {
    static ref FORMAT_REGISTRY: RwLock<FormatRegistry> = {
        let mut registry = FormatRegistry::new();
        registry.register_format("json", Box::new(JsonFormat::new()));
        RwLock::new(registry)
    };
}
```

## Example: Implementing JSON Format

Here's a complete example of adding JSON format support:

```rust
// src/formats/json_format.rs
use anyhow::Result;
use arrow::json::ReaderBuilder;
use bytes::Bytes;
use datafusion::dataframe::DataFrame;

pub struct JsonFormat {
    config: JsonConfig,
}

impl JsonFormat {
    pub fn new(config: JsonConfig) -> Self {
        Self { config }
    }
}

impl DataFormat for JsonFormat {
    fn read(&self, data: &Bytes) -> Result<DataFrame> {
        let reader = ReaderBuilder::new()
            .with_batch_size(self.config.batch_size)
            .build(std::io::Cursor::new(data))?;
        
        let batches: Vec<RecordBatch> = reader.collect()?;
        DataFrame::new(batches)
    }

    fn write(&self, df: &DataFrame) -> Result<Bytes> {
        let mut buf = Vec::new();
        let writer = arrow::json::Writer::new(&mut buf);
        
        for batch in df.collect()?.iter() {
            writer.write(batch)?;
        }
        
        Ok(Bytes::from(buf))
    }
}
```

## Format-Specific Features

### CSV Format Features
- Automatic schema inference
- Custom delimiter support
- Header row handling
- Type inference and casting
- NULL value handling

### Parquet Format Features
- Column compression
- Statistics collection
- Predicate pushdown support
- Row group size optimization
- Dictionary encoding

## Best Practices for Format Implementation

1. **Error Handling**
   - Use descriptive error messages
   - Implement custom error types if needed
   - Provide context for failures

2. **Performance**
   - Use streaming where possible
   - Implement efficient memory management
   - Consider chunking for large files

3. **Testing**
   - Unit tests for format-specific logic
   - Integration tests with real data
   - Performance benchmarks

4. **Configuration**
   - Make format options configurable
   - Provide sensible defaults
   - Document all configuration options

## Future Format Support

Planned format support includes:
- JSON (both line-delimited and array formats)
- Avro
- ORC
- Arrow IPC
- Custom binary formats

Each format implementation should consider:
- Schema handling
- Type conversion
- Compression options
- Performance characteristics
- Memory usage patterns
