# Builder stage
FROM rust:1.93-bookworm AS builder
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cargo build --release -p flushdb-server -p flushdb-demo

# Server stage
FROM debian:bookworm-slim AS server
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/flushdb-server /usr/local/bin/flushdb-server
EXPOSE 50051 9090
ENTRYPOINT ["flushdb-server"]

# Demo stage
FROM debian:bookworm-slim AS demo
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/flushdb-demo /usr/local/bin/flushdb-demo
ENTRYPOINT ["flushdb-demo"]
