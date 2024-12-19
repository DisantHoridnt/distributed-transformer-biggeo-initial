# Development Guide

## Project Structure

```
rs_app/
├── src/
│   ├── formats/
│   │   ├── mod.rs
│   │   ├── csv_format.rs
│   │   └── parquet_format.rs
│   ├── storage/
│   │   ├── mod.rs
│   │   ├── s3.rs
│   │   ├── local.rs
│   │   └── azure.rs
│   ├── execution/
│   │   └── mod.rs
│   └── main.rs
├── Cargo.toml
└── .env
```

## Adding New Features

### Adding a New Storage Backend

1. Create a new file in `src/storage/`
2. Implement the `Storage` trait:
```rust
pub trait Storage: Send + Sync {
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn read(&self, url: &Url) -> Result<Box<dyn Stream<Item = Result<Bytes>>>>;
    async fn read_all(&self, url: &Url) -> Result<Bytes>;
}
```

### Adding a New Format

1. Create a new file in `src/formats/`
2. Implement the `DataFormat` trait:
```rust
pub trait DataFormat: Send + Sync {
    fn read(&self, data: &Bytes) -> Result<DataFrame>;
    fn write(&self, df: &DataFrame) -> Result<Bytes>;
}
```

## Testing

Run tests:
```bash
cargo test
```

## Code Style

- Follow Rust standard naming conventions
- Use `rustfmt` for formatting
- Run `clippy` for linting:
```bash
cargo clippy
```

## Error Handling

- Use the `anyhow` crate for error handling
- Implement custom errors when needed
- Provide descriptive error messages

## Logging

- Use the `log` crate for logging
- Configure log levels appropriately
- Include relevant context in log messages

## Performance Considerations

- Use async/await for I/O operations
- Implement streaming where possible
- Consider memory usage with large files
- Use DataFusion for efficient data processing
