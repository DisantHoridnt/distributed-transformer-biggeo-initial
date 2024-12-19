# Distributed Transformer

A high-performance, streaming ETL system built in Rust that supports multiple storage backends and file formats.

## Architecture

### Format-Agnostic Data Processing

The system is built around a flexible, trait-based architecture that enables format-agnostic data processing:

```rust
#[async_trait]
pub trait DataFormat: Send + Sync {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>>;
    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes>;
    fn clone_box(&self) -> Box<dyn DataFormat + Send + Sync>;
}
```

#### Supported Formats

1. **Parquet Format**
   - Full streaming support for reading and writing
   - Integration with Arrow's Parquet implementation
   - Efficient columnar storage and compression

2. **CSV Format**
   - Automatic schema inference with type detection
   - Configurable options:
     - Header row handling
     - Batch size for streaming
     - Custom delimiters (planned)
   - Type conversion and validation

### DataFusion Integration

The system integrates deeply with DataFusion for efficient query processing:

1. **Custom Table Provider**
   ```rust
   pub struct FormatTableProvider {
       format: Box<dyn DataFormat + Send + Sync>,
       storage: Arc<dyn Storage>,
       data: Option<Bytes>,
       schema: SchemaRef,
   }
   ```

2. **Streaming Execution Engine**
   - Custom `ExecutionPlan` implementation
   - Lazy evaluation of data
   - Push-down optimization support

### Key Features

1. **Streaming Processing**
   - Memory-efficient handling of large datasets
   - Asynchronous I/O throughout the pipeline
   - Backpressure support via Rust's Stream trait

2. **Query Optimization**
   - Predicate pushdown
   - Column projection
   - Lazy evaluation of queries

3. **Format Registry**
   - Dynamic format selection based on file extensions
   - Extensible architecture for adding new formats
   ```rust
   lazy_static! {
       static ref FORMAT_REGISTRY: HashMap<&'static str, FormatFactory> = {
           let mut m = HashMap::new();
           m.insert("parquet", || Box::new(ParquetFormat));
           m.insert("csv", || Box::new(CsvFormat::new(true, 1024)));
           m
       };
   }
   ```

4. **Storage Abstraction**
   - Support for multiple storage backends
   - Async I/O operations
   - Streaming data transfer

## Usage

### Basic Example

```rust
let input_url = "s3://bucket/data.csv";
let output_url = "s3://bucket/output.parquet";

// Process data with SQL transformation
process_data(
    storage,
    &input_url,
    &output_url,
    Some("SELECT * FROM data WHERE age > 25")
).await?;
```

### Format-Specific Features

#### Parquet
- Efficient columnar storage
- Compression support
- Schema preservation
- Predicate pushdown support

#### CSV
- Automatic schema inference
- Header row support
- Type detection for:
  - Integer (i64)
  - Float (f64)
  - String (Utf8)
- Configurable batch size for memory control

## Implementation Details

### Execution Pipeline

1. **Format Detection**
   - File extension based format selection
   - Dynamic format instantiation

2. **Schema Management**
   - Automatic schema inference for schemaless formats
   - Schema validation and preservation
   - Type conversion handling

3. **Query Execution**
   - Custom `ExecutionPlan` for streaming
   - Integration with DataFusion's query optimizer
   - Predicate and projection pushdown

4. **Memory Management**
   - Streaming-first architecture
   - Batch-based processing
   - Configurable batch sizes

### Performance Optimizations

1. **Lazy Evaluation**
   - Data is only read when needed
   - Query predicates are pushed down
   - Column projection minimizes I/O

2. **Streaming Processing**
   - Constant memory usage regardless of data size
   - Backpressure support
   - Asynchronous I/O

3. **Resource Management**
   - Connection pooling for storage backends
   - Batch size tuning
   - Async task management

## Future Enhancements

1. **Format Support**
   - JSON format implementation
   - Arrow IPC format
   - Custom format plugins

2. **Query Optimization**
   - Advanced predicate pushdown
   - Statistics-based optimization
   - Parallel query execution

3. **Configuration**
   - Format-specific configuration
   - Performance tuning parameters
   - Resource limits

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

## Weather Data Processing Workflow

This section describes how to use the application for processing weather data stored in Parquet format.

### Data Processing Pipeline

The application implements a data processing pipeline that:
1. Reads Parquet files from S3
2. Applies SQL filters using DataFusion
3. Displays results in the terminal
4. Optionally writes filtered results back to S3

### Usage Example

```bash
# Process weather data with SQL filter
cargo run -- \
  --input-url s3://book-images-repo/input/weather.parquet \
  --output-url s3://book-images-repo/output/weather_filtered.parquet \
  --filter-sql "SELECT * FROM data LIMIT 10"
```

This command will:
1. Read the weather data from the input S3 path
2. Apply the SQL filter to limit to 10 rows
3. Display the filtered data in a formatted table
4. Write the results to the output S3 path

Example output:
```
Applying SQL filter: SELECT * FROM data LIMIT 10

Filtered data:
+---------+---------+----------+-------------+----------+-------------+---------------+------------+------------+--------------+--------------+-------------+-------------+-------------+-------------+----------+----------+---------+---------+-----------+---------+--------------+
| MinTemp | MaxTemp | Rainfall | Evaporation | Sunshine | WindGustDir | WindGustSpeed | WindDir9am | WindDir3pm | WindSpeed9am | WindSpeed3pm | Humidity9am | Humidity3pm | Pressure9am | Pressure3pm | Cloud9am | Cloud3pm | Temp9am | Temp3pm | RainToday | RISK_MM | RainTomorrow |
+---------+---------+----------+-------------+----------+-------------+---------------+------------+------------+--------------+--------------+-------------+-------------+-------------+-------------+----------+----------+---------+---------+-----------+---------+--------------+
| 8.0     | 24.3    | 0.0      | 3.4         | 6.3      | NW          | 30            | SW         | NW         | 6            | 20           | 68          | 29          | 1019.7      | 1015.0      | 7        | 7        | 14.4    | 23.6    | No        | 3.6     | Yes          |
...
```

### Features

- **SQL Filtering**: Apply SQL queries to filter and transform your data
- **Pretty Printing**: Displays data in a formatted table in the terminal
- **Storage Support**: Works with both local filesystem and S3 storage
- **Asynchronous Processing**: Efficient handling of I/O operations

### Example SQL Queries

```sql
-- Display first 10 rows
SELECT * FROM data LIMIT 10

-- Filter by specific conditions
SELECT * FROM data WHERE temperature > 25

-- Aggregate data
SELECT AVG(temperature) as avg_temp, 
       MAX(temperature) as max_temp,
       MIN(temperature) as min_temp 
FROM data
```

### Error Handling

The application provides detailed error messages for common issues:
- S3 connectivity problems
- Invalid SQL queries
- File access permissions
- Data format inconsistencies

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