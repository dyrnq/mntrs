# Build stage
FROM rust:1.87-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler libfuse3-dev fuse3 pkg-config && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY csi ./csi

RUN cargo build --package mntrs-csi --release --locked && \
    cp target/release/mntrs-csi /mntrs-csi

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates fuse3 libfuse3-3 tini && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /mntrs-csi /bin/mntrs-csi

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/mntrs-csi"]
