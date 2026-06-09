FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_VERSION=3.3.6

RUN apt-get update && apt-get install -y \
    openjdk-21-jdk-headless \
    krb5-kdc krb5-admin-server krb5-user \
    openssh-server openssh-client \
    curl wget \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sL "https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    | tar xz -C /opt && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop

RUN ssh-keygen -t rsa -P "" -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/authorized_keys && \
    ssh-keyscan -H localhost >> /root/.ssh/known_hosts

COPY krb5.conf /etc/krb5.conf

RUN /usr/sbin/krb5_newrealm <<< "adminpass"$'\n'"adminpass" && \
    kadmin.local -q "addprinc -randkey hdfs/localhost@TEST.LOCAL" && \
    kadmin.local -q "addprinc -randkey HTTP/localhost@TEST.LOCAL" && \
    kadmin.local -q "addprinc -randkey testuser@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/hdfs.keytab hdfs/localhost@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/hdfs.keytab HTTP/localhost@TEST.LOCAL" && \
    kadmin.local -q "ktadd -k /tmp/testuser.keytab testuser@TEST.LOCAL" && \
    chmod 644 /tmp/hdfs.keytab /tmp/testuser.keytab

COPY hdfs-site.xml core-site.xml /opt/hadoop/etc/hadoop/

RUN echo "export JAVA_HOME=\${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> /opt/hadoop/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_OPTS=\"-Djava.security.krb5.conf=/etc/krb5.conf\"" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

RUN /opt/hadoop/bin/hdfs namenode -format -force

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8020 9870 88 749

ENTRYPOINT ["/entrypoint.sh"]
