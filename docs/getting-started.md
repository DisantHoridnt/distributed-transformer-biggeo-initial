# Getting Started

## Prerequisites

- Rust (latest stable version)
- AWS Account with S3 access
- Environment variables configured

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/distributed-transformer.git
cd distributed-transformer
```

2. Set up environment variables in `.env`:
```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1
```

3. Build the project:
```bash
cd rs_app
cargo build --release
```

## Usage

Convert a CSV file to Parquet format:
```bash
cargo run -- convert --input s3://your-bucket/input/file.csv --output s3://your-bucket/output/file.parquet
```

## Example

```bash
cargo run -- convert --input s3://book-images-repo/input/weather.csv --output s3://book-images-repo/output/weather.parquet
```
