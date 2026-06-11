FROM alpine:3.21

# mntrs-csi (musl static) binary will be copied at build time.
# fuse3-libs provides libfuse3.so for the spawned FUSE mount process.
RUN apk add --no-cache fuse3-libs

COPY mntrs-csi /usr/local/bin/mntrs-csi

ENTRYPOINT ["/usr/local/bin/mntrs-csi"]
CMD ["--help"]
