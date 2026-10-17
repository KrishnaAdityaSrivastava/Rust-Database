FROM rust:1-bookworm AS builder

WORKDIR /app

# Install the Protocol Buffers compiler
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy Cargo manifests and protobuf build inputs
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./build.rs
COPY proto ./proto

# Copy application source and tests
COPY src ./src
COPY tests ./tests

# Generate protobuf code and compile the node
RUN cargo build --release --bin node

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/node /usr/local/bin/node

EXPOSE 6001 7001 8001

ENTRYPOINT ["/usr/local/bin/node"]
