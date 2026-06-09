#!/bin/bash
set -e

# Start SSH
service ssh start

# Start KDC
service krb5-kdc start
service krb5-admin-server start

# Verify KDC
echo "Testing KDC..."
echo | kinit -kt /tmp/hdfs.keytab hdfs/localhost@TEST.LOCAL && echo "KDC OK" || echo "KDC FAILED"

# Start HDFS
echo "Starting NameNode..."
/opt/hadoop/bin/hdfs --daemon start namenode
sleep 5

echo "Starting DataNode..."
/opt/hadoop/bin/hdfs --daemon start datanode
sleep 5

# Verify
echo "=== HDFS Report ==="
/opt/hadoop/bin/hdfs dfsadmin -report 2>&1 | head -15

echo "=== HDFS Ready ==="
# Keep container alive
tail -f /dev/null
