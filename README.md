# Distributed Transformer

A distributed ETL pipeline for transforming large datasets using Rust, with support for multiple storage backends (Local, S3, Azure).

## Prerequisites

- Rust
- Docker
- kubectl
- Argo Workflows
- AWS CLI (for S3 storage)
- Azure CLI (for Azure storage)

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/DisantHoridnt/distributed-transformer.git
cd distributed-transformer
```

2. Set up environment variables:
```bash
# Copy the example .env file
nano rs_app/.env
```

Required environment variables in `.env`:
```env
# AWS
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_REGION=your_aws_region
S3_BUCKET_NAME=your_bucket_name

# Azure (if using Azure storage)
AZURE_STORAGE_ACCOUNT=your_azure_account
AZURE_STORAGE_ACCESS_KEY=your_azure_key

# Test Configuration
TEST_S3_BUCKET=your_test_bucket
TEST_AZURE_CONTAINER=your_test_container
```

## Development

### Building

```bash
# Format code
make fmt

# Run lints
make lint

# Build the project
make build

# Clean build artifacts
make clean
```

### Testing

```bash
# Run local storage tests
make test-local

# Run S3 storage tests (requires AWS credentials)
make test-s3

# Run Azure storage tests (requires Azure credentials)
make test-azure

# Run all tests
make test

# Generate test coverage report
make coverage
```

### Running Locally

```bash
# Run with default configuration
make run

# Run with custom parameters
make run INPUT_URL="s3://mybucket/input.parquet" \
         OUTPUT_URL="s3://mybucket/output.parquet" \
         SQL_FILTER="SELECT * FROM data WHERE column > 100"

# Watch for changes and rebuild
make watch
```

## Docker

```bash
# Build Docker image
make build

# Push to registry
make push

# Build with custom tag
make build IMAGE_TAG=custom-tag
```

## Kubernetes Deployment

### Prerequisites
1. A running Kubernetes cluster
2. Argo Workflows installed
3. kubectl configured with cluster access
4. Docker registry access

### Deployment Steps

1. Deploy to Kubernetes:
```bash
# Deploy with default configuration
make k8s-deploy

# Deploy with custom parameters
make k8s-deploy \
    INPUT_URL="s3://mybucket/input.parquet" \
    OUTPUT_URL="s3://mybucket/output.parquet" \
    SQL_FILTER="SELECT * FROM data WHERE column > 100" \
    K8S_NAMESPACE="my-namespace"
```

2. Delete deployment:
```bash
make k8s-delete
```

## Storage Backend URLs

The application supports different storage backends through URLs:

- Local storage: `file:///path/to/file.parquet`
- S3 storage: `s3://bucket-name/path/to/file.parquet`
- Azure storage: `azure://container-name/path/to/file.parquet`

## Makefile Commands

Run `make help` to see all available commands:

- `make all`: Build and test everything
- `make build`: Build the Docker image
- `make push`: Push the Docker image to registry
- `make run`: Run locally
- `make test`: Run all tests
- `make clean`: Clean build artifacts
- `make fmt`: Format Rust code
- `make lint`: Run clippy lints
- `make coverage`: Generate test coverage report
- `make k8s-deploy`: Deploy to Kubernetes
- `make k8s-delete`: Delete Kubernetes resources
- `make version`: Display version information
- `make watch`: Watch for file changes and rebuild

## Troubleshooting

### Common Issues

1. **AWS Credentials**
   - Ensure AWS credentials are properly set in `.env`
   - Verify AWS CLI configuration
   - Check S3 bucket permissions

2. **Azure Storage**
   - Verify Azure credentials in `.env`
   - Check container permissions
   - Ensure Azure CLI is configured

3. **Kubernetes Deployment**
   - Verify cluster access: `kubectl cluster-info`
   - Check pod logs: `kubectl logs -n <namespace> <pod-name>`
   - Verify Argo workflow status: `argo list -n <namespace>`

### Logs

- Application logs: Available through `kubectl logs` or Argo UI
- Test logs: Use `RUST_LOG=debug` for verbose logging
- Build logs: Check Docker build output