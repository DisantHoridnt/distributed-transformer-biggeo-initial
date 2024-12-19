# Architecture

## Overview

The Distributed Transformer is built using a modular architecture that separates concerns between storage, format handling, and execution.

## Components

### Storage Module
- Located in `src/storage/`
- Handles interactions with different storage backends
- Currently supports:
  - S3 Storage (`s3.rs`)
  - Local Storage (`local.rs`)
  - Azure Storage (`azure.rs`) [Placeholder]

### Format Module
- Located in `src/formats/`
- Manages different file formats
- Supports:
  - CSV (`csv_format.rs`)
  - Parquet (`parquet_format.rs`)

### Execution Module
- Located in `src/execution/`
- Handles the conversion process
- Uses DataFusion for efficient data processing

## Data Flow

1. Input file is read from storage (S3/Local)
2. Data is processed through DataFusion
3. Converted data is written to the output location

## Dependencies

Key dependencies include:
- `datafusion`: SQL query engine and DataFrame library
- `object_store`: Storage abstraction layer
- `arrow`: Data processing and memory layout
- `parquet`: Parquet file format handling
- `tokio`: Async runtime
