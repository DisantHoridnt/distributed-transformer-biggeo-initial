FROM rust:1.72 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/distributed-transformer /app/distributed-transformer
COPY .env /app/.env
CMD ["/app/distributed-transformer"]
