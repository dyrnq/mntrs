FROM debian:trixie-slim

# Runtime deps: fuse3 for FUSE mount, krb5 for HDFS Kerberos GSSAPI auth.
# hdfs-native's libloading looks for unversioned .so names, so create
# symlinks from the versioned files shipped in the distro packages.
RUN apt-get update && apt-get install -y --no-install-recommends \
    fuse3 \
    libfuse3-4 \
    libgssapi-krb5-2 \
    libkrb5-3 \
    libk5crypto3 \
    libcom-err2 \
    libgssrpc4t64 \
    libkrb5support0 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && LIBDIR=$(ls -d /usr/lib/*-linux-gnu*) \
    && ln -s libgssapi_krb5.so.2   ${LIBDIR}/libgssapi_krb5.so \
    && ln -s libkrb5.so.3          ${LIBDIR}/libkrb5.so \
    && ln -s libk5crypto.so.3      ${LIBDIR}/libk5crypto.so \
    && ln -s libcom_err.so.2       ${LIBDIR}/libcom_err.so \
    && ln -s libgssrpc.so.4        ${LIBDIR}/libgssrpc.so \
    && ln -s libkrb5support.so.0   ${LIBDIR}/libkrb5support.so

COPY mntrs-csi /usr/local/bin/mntrs-csi

ENTRYPOINT ["/usr/local/bin/mntrs-csi"]
CMD ["--help"]
