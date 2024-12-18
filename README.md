# Distributed Transformer

A high-performance ETL tool built in Rust for processing large-scale Parquet files using Apache Arrow and DataFusion. This tool is designed to run in Kubernetes using Argo Workflows for orchestration.

## Features

- 🚀 **High Performance**: Built with Rust for maximum efficiency
- 📊 **Parquet Support**: Read and write Apache Parquet files
- 🔍 **SQL Filtering**: Apply SQL filters using DataFusion
- ☁️ **Cloud Native**: Direct S3 integration
- 🔄 **Streaming**: Asynchronous streaming of record batches
- 🎯 **Kubernetes Ready**: Containerized and ready for K8s deployment
- 🔧 **Easy Configuration**: Environment variables and CLI parameters

## Prerequisites

- Rust
- Docker
- Kubernetes cluster
- Argo Workflows
- AWS credentials (for S3 access)

## Project Structure

```
.
├── k8s/                    # Kubernetes/Argo configurations
│   └── workflow.yaml       # Argo workflow definition
├── rs_app/                 # Rust application
│   ├── src/               # Source code
│   ├── Cargo.toml         # Rust dependencies
│   ├── Dockerfile         # Container definition
│   ├── .env              # Environment variables
│   └── VERSION           # Application version
└── Makefile              # Build and deployment automation
```

## Quick Start

1. **Configure AWS Credentials**

   Create a `.env` file in the `rs_app` directory:
   ```env
   AWS_ACCESS_KEY_ID=your_access_key
   AWS_SECRET_ACCESS_KEY=your_secret_key
   AWS_REGION=your_region
   ```

2. **Build and Deploy**

   ```bash
   # Build the Docker image and deploy to Kubernetes
   make k8s-deploy
   ```

   Or with custom parameters:
   ```bash
   make k8s-deploy \
     INPUT_URL=s3://my-bucket/input.parquet \
     OUTPUT_URL=s3://my-bucket/output.parquet \
     SQL_FILTER="SELECT * FROM data WHERE value > 100"
   ```

## Development

### Available Make Commands

- `make build` - Build the Docker image
- `make test` - Run tests
- `make fmt` - Format Rust code
- `make run` - Submit Argo workflow
- `make clean` - Clean up build artifacts
- `make watch` - Watch for changes and rebuild
- `make k8s-deploy` - Build and deploy to Kubernetes
- `make k8s-delete` - Delete workflows from Kubernetes

### Local Development

1. **Setup Development Environment**
   ```bash
   cd rs_app
   cargo build
   ```

2. **Run Tests**
   ```bash
   cargo test
   ```

3. **Format Code**
   ```bash
   cargo fmt
   ```

## Architecture

### Components

1. **BytesReader & SyncReader**
   - Custom implementations for reading Parquet files
   - Supports both synchronous and asynchronous operations
   - Efficient memory management for large files

2. **SQL Processing**
   - Uses DataFusion for SQL query execution
   - Supports filtering and transformation of data
   - Memory-efficient batch processing

3. **S3 Integration**
   - Direct integration with S3 using object_store
   - Streaming support for large files
   - Configurable through environment variables

### Workflow

1. Read Parquet file from S3
2. Stream record batches
3. Apply SQL transformations (optional)
4. Write results back to S3

## Configuration

### Environment Variables

Put it all in rs_app/.env

- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_REGION` - AWS region

### CLI Parameters

- `--input-url` - S3 URL for input Parquet file
- `--output-url` - S3 URL for output file
- `--filter-sql` - Optional SQL filter