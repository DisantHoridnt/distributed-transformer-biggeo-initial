# Build stage
FROM rust:1.72 as builder
WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y cmake pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy only the dependency files first
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Now copy the actual source code
COPY . .

# Build the actual application
RUN touch src/main.rs && \
    cargo build --release && \
    cargo test --release

# Runtime stage
FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash app
USER app

WORKDIR /app

# Copy the binary and configuration
COPY --from=builder --chown=app:app /app/target/release/distributed-transformer /app/distributed-transformer
COPY --chown=app:app .env /app/.env

# Create directory for temporary files
RUN mkdir -p /app/tmp && chown app:app /app/tmp

# Set environment variables
ENV RUST_LOG=info
ENV TMPDIR=/app/tmp
ENV DOTENV_PATH=/app/.env

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the application
ENTRYPOINT ["/app/distributed-transformer"]
