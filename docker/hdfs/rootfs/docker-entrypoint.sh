#!/usr/bin/env bash
set -eo pipefail

# Fix SSH permissions
chmod g-w /root
chmod o-w /root
service ssh start 2>/dev/null || true

# Ensure KDC is ready
echo "Waiting for KDC..."
for i in $(seq 1 10); do
    if echo | kinit -kt /tmp/hdfs.keytab hdfs/localhost@TEST.LOCAL 2>/dev/null; then
        echo "KDC ready"
        break
    fi
    echo "  attempt $i..."
    sleep 3
done

exec /init
