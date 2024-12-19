# API Reference

## Command Line Interface

### Convert Command

Convert a CSV file to Parquet format.

```bash
cargo run -- convert --input <input_path> --output <output_path>
```

#### Parameters:
- `--input`: Source CSV file path (S3 URL)
- `--output`: Destination Parquet file path (S3 URL)

## Core Traits

### Storage Trait

```rust
pub trait Storage: Send + Sync {
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn read(&self, url: &Url) -> Result<Box<dyn Stream<Item = Result<Bytes>>>>;
    async fn read_all(&self, url: &Url) -> Result<Bytes>;
}
```

### DataFormat Trait

```rust
pub trait DataFormat: Send + Sync {
    fn read(&self, data: &Bytes) -> Result<DataFrame>;
    fn write(&self, df: &DataFrame) -> Result<Bytes>;
}
```

## Storage Implementations

### S3Storage

```rust
pub struct S3Storage {
    store: Box<dyn ObjectStore>,
    bucket: String,
}

impl S3Storage {
    pub fn new(bucket: String) -> Result<Self>;
}
```

Configuration options:
- Region
- Endpoint
- Credentials
- Skip signature verification

### LocalStorage

```rust
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: PathBuf) -> Self;
}
```

## Format Implementations

### CsvFormat

```rust
pub struct CsvFormat {
    config: CsvConfig,
}

impl CsvFormat {
    pub fn new(config: CsvConfig) -> Self;
}
```

### ParquetFormat

```rust
pub struct ParquetFormat {
    config: ParquetConfig,
}

impl ParquetFormat {
    pub fn new(config: ParquetConfig) -> Self;
}
```

## Error Types

Common error types returned by the API:
- `StorageError`: Issues with storage operations
- `FormatError`: Issues with format conversion
- `ConfigError`: Configuration-related errors
- `IoError`: Input/output operation errors
