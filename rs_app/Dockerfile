FROM rust:1.72 as builder
WORKDIR /app

# Copy only the dependency files first
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Now copy the actual source code
COPY . .
# Touch main.rs to ensure it's newer than the dummy one
RUN touch src/main.rs && \
    cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/distributed-transformer /app/distributed-transformer
COPY .env /app/.env
CMD ["/app/distributed-transformer"]
