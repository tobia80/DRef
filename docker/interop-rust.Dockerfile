# syntax=docker/dockerfile:1.6
# Rust node for the cross-language DRef interop demo.
# Used by docker-compose.interop.yml together with docker/interop-scala.Dockerfile.

FROM rust:1-bookworm AS build
WORKDIR /app

# protoc is required by tonic-prost-build (dref-raft compiles protobuf schemas).
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Cargo workspace + crates needed to build interop-node.
COPY rust/Cargo.toml rust/Cargo.lock ./rust/
COPY rust/dref-core ./rust/dref-core
COPY rust/dref-raft ./rust/dref-raft
COPY rust/dref-redis ./rust/dref-redis
COPY rust/examples ./rust/examples
COPY rust/interop-node ./rust/interop-node

# The protobuf definitions live at the repo root and are referenced by
# rust/dref-raft/build.rs as `../proto/...`.
COPY proto ./proto

WORKDIR /app/rust
RUN cargo build --release -p interop-node

FROM debian:bookworm-slim
WORKDIR /opt/dref
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/rust/target/release/interop-node /usr/local/bin/interop-node
ENV DREF_PORT=8082
EXPOSE 8082
ENTRYPOINT ["interop-node"]
