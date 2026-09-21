# Build stage
FROM rust:1.91-slim-bookworm AS builder

# protobuf-compiler's apt `Recommends` is libprotobuf-dev (the
# /usr/include/google/protobuf/*.proto include dir tonic-prost-build's
# `compile_protos("csi.proto")` needs). `--no-install-recommends` would
# strip it — protoc binary would be present but every well-known .proto
# import ("google/protobuf/descriptor.proto" etc.) would fail with
# "File not found" (release-image run 35576793117).
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler libprotobuf-dev libfuse3-dev fuse3 pkg-config && \
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
