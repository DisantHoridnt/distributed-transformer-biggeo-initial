# Distributed Transformer Documentation

This documentation provides comprehensive information about the Distributed Transformer project, a Rust-based ETL application for converting CSV files to Parquet format using AWS S3 storage.

## Quick Start

### Converting CSV to Parquet
To convert a CSV file to Parquet format:
```bash
cargo run -- convert --input s3://book-images-repo/input/weather.csv --output s3://book-images-repo/output/weather.parquet
```

### Filtering Parquet Data
To filter and transform Parquet data:
```bash
cargo run -- convert --input s3://book-images-repo/input/weather.parquet --output s3://book-images-repo/output/weather_filtered.parquet --filter-sql true
```

The `--filter-sql` option accepts SQL WHERE clauses to filter the data. For example:
- `true` - Select all records (with a limit of 10)
- `temperature > 25` - Select records where temperature is greater than 25
- `city = 'London'` - Select records for a specific city

## Table of Contents

1. [Getting Started](./getting-started.md)
2. [Architecture](./architecture.md)
3. [AWS Configuration](./aws-configuration.md)
4. [Development Guide](./development-guide.md)
5. [API Reference](./api-reference.md)
