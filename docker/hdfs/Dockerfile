FROM ubuntu:24.04

ARG S6_OVERLAY_VERSION=3.2.0.2
ARG HADOOP_VERSION=3.5.0

# Hostname baked into the kerberos principals. CI's hdfs-kerberos job
# must pass --hostname matching this when running the container,
# otherwise the NameNode binds to the wrong address and SASL auth
# fails with "Connection reset by peer". Default 'hdfs.test' is a
# single-label hostname that won't be mangled by FQDN resolvers.
ARG HDFS_HOSTNAME=hdfs.test

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=en_US.utf8 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    HADOOP_HOME=/opt/hadoop \
    S6_OVERLAY_VERSION=${S6_OVERLAY_VERSION} \
    HDFS_HOSTNAME=${HDFS_HOSTNAME}

# Local: use USTC mirror + HTTP proxy (DO NOT COMMIT)

RUN set -ex; \
    apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk-headless \
    krb5-kdc krb5-admin-server krb5-user \
    curl wget ca-certificates procps netcat-openbsd gosu \
    locales xz-utils; \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8; \
    rm -rf /var/lib/apt/lists/*

RUN set -ex; \
    ARCH="$(uname -m)"; \
    case "${ARCH}" in \
        aarch64|arm64) s6arch="aarch64" ;; \
        amd64|x86_64)  s6arch="x86_64" ;; \
        *) echo "Unsupported arch: ${ARCH}"; exit 1 ;; \
    esac; \
    curl --retry 3 -f#SL "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-${s6arch}.tar.xz" | tar -Jxp -C /; \
    curl --retry 3 -f#SL "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz" | tar -Jxp -C /; \
    curl --retry 3 -f#SL "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-symlinks-noarch.tar.xz" | tar -Jxp -C /

RUN curl --retry 3 -f#SL "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar xz -C /opt && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

# Create app user (uid 1000 matches host)
# Use ubuntu user (uid 1000) — already exists in base image
RUN if id ubuntu 2>/dev/null; then \
        groupmod -n mntrs ubuntu && \
        usermod -l mntrs -d /home/mntrs -m ubuntu; \
    else \
        groupadd -g 1000 -o mntrs && \
        useradd -u 1000 -g 1000 -m -s /bin/bash mntrs; \
    fi




COPY krb5.conf /etc/krb5.conf

RUN touch /etc/krb5kdc/kadm5.acl
RUN printf "adminpass\nadminpass\n" | /usr/sbin/krb5_newrealm && \
    kadmin.local -q "addprinc -randkey hdfs/${HDFS_HOSTNAME}@TEST.LOCAL" && \
    kadmin.local -q "addprinc -randkey HTTP/${HDFS_HOSTNAME}@TEST.LOCAL" && \
    kadmin.local -q "addprinc -randkey testuser@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/hdfs.keytab hdfs/${HDFS_HOSTNAME}@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/hdfs.keytab HTTP/${HDFS_HOSTNAME}@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/testuser.keytab testuser@TEST.LOCAL" && \
    chmod 644 /tmp/hdfs.keytab /tmp/testuser.keytab

COPY hdfs-site.xml core-site.xml /opt/hadoop/etc/hadoop/

# The default hdfs-site.xml/cert from this repo pin the kerberos
# principal to hdfs/localhost — but the image is now built with a
# parameterized hostname baked in. Substitute the file's hostname
# (only the principal strings; everything else stays as-is) so the
# server and client agree on the service principal.
RUN sed -i "s|hdfs/localhost|hdfs/${HDFS_HOSTNAME}|g; s|HTTP/localhost|HTTP/${HDFS_HOSTNAME}|g" \
    /opt/hadoop/etc/hadoop/hdfs-site.xml /opt/hadoop/etc/hadoop/core-site.xml

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=mntrs" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=mntrs" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=mntrs" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo 'export HADOOP_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"' >> /opt/hadoop/etc/hadoop/hadoop-env.sh

ARG CACHE_BUSTER=1

# Generate self-signed keystore for HTTPS (DataNode Kerberos requirement).
# DName CN must match the kerberos principal hostname for HTTPS handlers
# that do hostname verification.
RUN keytool -genkey -alias hdfs -keyalg RSA -keysize 2048 -keystore /tmp/keystore.jks \
    -dname "CN=${HDFS_HOSTNAME}" -storepass changeit -keypass changeit -noprompt -validity 3650 \
    && chmod 644 /tmp/keystore.jks

COPY rootfs /

RUN \
mkdir -p /var/lib/hadoop && \
chown -R mntrs:mntrs /var/lib/hadoop && \
chown -R mntrs:mntrs /opt/hadoop; mkdir -p /opt/hadoop/logs; chown mntrs:mntrs /opt/hadoop/logs

EXPOSE 8020 9870 88 749 22

ENTRYPOINT ["/docker-entrypoint.sh"]
