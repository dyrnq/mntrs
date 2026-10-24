# Build stage
#
# Rust version: 1.99 — MUST stay in sync with the `stable` channel the
# CI workflows use (`dtolnay/rust-toolchain@stable`, currently 1.99.1).
#
# History:
#   1.87 — original pin, bumped for opendal 0.58's MSRV
#   1.91 — bumped for opendal 0.59's MSRV (commit ef3a2d7)
#   1.99 — bumped because `Atomic*::try_update` (used in
#          src/mem_limiter.rs + src/metrics.rs, commit 12d867d) only
#          stabilized in Rust 1.95.0 (2026-04-16). On 1.91 the build
#          died with E0658 "use of unstable library feature
#          `atomic_try_update`" — release-image run 40075594001 on
#          tag v0.0.1-alpha.3.
#
# The repo's MSRV is therefore 1.95 (try_update); 1.99 is used here to
# match CI exactly, so the Docker builder and CI compile the same
# Cargo.lock with the same compiler.
FROM rust:1.99-slim-bookworm AS builder

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
