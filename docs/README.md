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
cargo run -- convert --input s3://book-images-repo/input/weather.parquet --output s3://book-images-repo/output/weather_filtered.parquet --filter-sql "\"MaxTemp\" > 25"
```

The `--filter-sql` option accepts SQL WHERE clauses to filter the data. For example:
- `true` - Select all records (with a limit of 10)
- `"MaxTemp" > 25` - Select records where maximum temperature is greater than 25°C
- `"RainToday" = 'Yes'` - Select records where it rained today
- `"WindSpeed9am" > 20 AND "Humidity9am" < 50` - Complex conditions are supported

The results will be displayed in a formatted table in the CLI, showing:
1. The SQL query being executed
2. All columns and their values
3. Total number of matching rows
4. Confirmation when the filtered data is written to the output location

## Table of Contents

1. [Getting Started](./getting-started.md)
2. [Architecture](./architecture.md)
3. [AWS Configuration](./aws-configuration.md)
4. [Development Guide](./development-guide.md)
5. [API Reference](./api-reference.md)
