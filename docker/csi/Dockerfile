FROM alpine:3.21

# mntrs-csi static binary will be copied at build time
COPY mntrs-csi /usr/local/bin/mntrs-csi

ENTRYPOINT ["/usr/local/bin/mntrs-csi"]
CMD ["--help"]
