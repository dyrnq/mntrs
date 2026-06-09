FROM ubuntu:24.04

ARG S6_OVERLAY_VERSION=3.2.0.2
ARG HADOOP_VERSION=3.3.6

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=en_US.utf8 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    HADOOP_HOME=/opt/hadoop \
    S6_OVERLAY_VERSION=${S6_OVERLAY_VERSION}

RUN set -ex; \
    apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk-headless \
    krb5-kdc krb5-admin-server krb5-user \
    openssh-server openssh-client \
    curl wget ca-certificates procps netcat-openbsd \
    locales xz-utils; \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8; \
    rm -rf /var/lib/apt/lists/*

# Install s6-overlay (same pattern as docker-doris)
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

# Download Hadoop
RUN curl --retry 3 -f#SL "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar xz -C /opt && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

# SSH + Kerberos config
COPY krb5.conf /etc/krb5.conf

RUN ssh-keygen -t rsa -P "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/authorized_keys && \
    ssh-keyscan -H localhost >> /root/.ssh/known_hosts && \
    chmod g-w /root && chmod o-w /root

# Create KDC database and principals (must be at build time)
RUN /usr/sbin/krb5_newrealm <<< "adminpass"$'\n'"adminpass" && \
    kadmin.local -q "addprinc -randkey hdfs/localhost@TEST.LOCAL" && \
    kadmin.local -q "addprinc -randkey HTTP/localhost@TEST.LOCAL" && \
    kadmin.local -q "addprinc -randkey testuser@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/hdfs.keytab hdfs/localhost@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/hdfs.keytab HTTP/localhost@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/testuser.keytab testuser@TEST.LOCAL" && \
    chmod 644 /tmp/hdfs.keytab /tmp/testuser.keytab

# Hadoop config
COPY hdfs-site.xml core-site.xml /opt/hadoop/etc/hadoop/

RUN set -ex; \
    echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh; \
    echo "export HDFS_NAMENODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh; \
    echo "export HDFS_DATANODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh; \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh; \
    echo 'export HADOOP_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"' >> /opt/hadoop/etc/hadoop/hadoop-env.sh; \
    /opt/hadoop/bin/hdfs namenode -format -force

# s6 overlay services
COPY rootfs /

EXPOSE 8020 9870 88 749 22

ENTRYPOINT ["/docker-entrypoint.sh"]
