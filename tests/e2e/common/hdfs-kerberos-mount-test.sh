#!/usr/bin/env bash
#
# tests/e2e/common/hdfs-kerberos-mount-test.sh
#
# HDFS-Kerberos-specific mount + smoke test for integration.yml's
# `hdfs-kerberos` job. Replaces the 155-line inline block at
# .github/workflows/integration.yml:Test HDFS Kerberos mount.
#
# Kerberos auth has unique requirements that don't share with the
# plain mount_test (memory/s3/hdfs-simple in tests/e2e/common/mount-test.sh):
#
#   1. Pre-step: kinit as the hdfs principal (superuser) on the host
#      using /tmp/hdfs.keytab (extracted from the container by the
#      caller). Required before mntrs can read/write anything because
#      hdfs-native (libhdfs) authenticates the host process to NN.
#
#   2. Seed test data INSIDE the container (the container needs its
#      own kinit first because the host kinit doesn't reach into it).
#      /test gets chmod 777 so the daemon's mount-process uid can
#      write there later.
#
#   3. Hadoop config: HADOOP_CONF_DIR=/tmp/hadoop-conf (copied from
#      the container) + KRB5_CONFIG=/etc/krb5.conf + KRB5CCNAME=
#      propagated into the daemon child. hdfs-native falls back to
#      defaults if HADOOP_CONF_DIR is unset, and the defaults don't
#      include the test realm's KDC.
#
#   4. Readiness probe uses `cat <preexist_file>` not `ls`: hdfs-native
#      list_op trips FUSE readdir EIO on some backends during NN
#      startup; cat goes through read which is the stable path.
#
# Usage:
#   . tests/e2e/common/hdfs-kerberos-mount-test.sh
#   hdfs_kerberos_mount_test <mount_path> <hdfs_hostname> <realm> \
#       <preexist_file> <expected_text> [log_path]
#
#   defaults: mount_path=/mnt/hdfs, hdfs_hostname=hdfs.test,
#   realm=TEST.LOCAL, preexist_file=test/hello.txt,
#   expected_text="hello kerberos hdfs",
#   log_path=/tmp/mntrs-mount.log
#
# Returns 0 on full pass, 1 on any sub-test failure or mount-readiness
# timeout. Caller's `if: always()` Cleanup step handles FUSE unmount +
# docker rm + keytab shred.
#
# Security notes (from #162 audit M5 + H6):
#   - script body has NO `set -x` (M5). The original step printed
#     kinit / mount child env via `set -x`, leaking the krb5cc path
#     and ticket cache contents into CI logs.
#   - script does NOT call `shred -u /tmp/hdfs.keytab` itself (H6).
#     The keytab is shared with the caller's pre-step (kinit for
#     superuser ops); cleanup belongs to the same caller.

# Guard against double-include.
if [[ -n "${__HDFS_KERBEROS_MOUNT_TEST_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__HDFS_KERBEROS_MOUNT_TEST_LOADED=1

hdfs_kerberos_mount_test() {
    local mount_path="${1:-/mnt/hdfs}"
    local hdfs_hostname="${2:-hdfs.test}"
    local realm="${3:-TEST.LOCAL}"
    local preexist_file="${4:-test/hello.txt}"
    local expected_text="${5:-hello kerberos hdfs}"
    local log_path="${6:-/tmp/mntrs-mount.log}"
    local principal="hdfs/${hdfs_hostname}@${realm}"

    # 1. Renew host-side ticket as the hdfs principal. The keytab is
    #    extracted by the caller's "Extracting Kerberos config" step
    #    from the container's /etc/hadoop/hdfs.keytab.
    kinit -kt /tmp/hdfs.keytab "$principal"
    klist

    # 2. Seed test data inside the container. The container needs its
    #    own kinit first because the host's ticket doesn't reach into
    #    it (different /tmp/krb5ccname).
    docker exec -u hdfs hdfs /usr/bin/kinit -kt /etc/hadoop/hdfs.keytab "$principal"
    docker exec -u hdfs hdfs /opt/hadoop/bin/hdfs dfs -mkdir -p /test
    docker exec -u hdfs hdfs /opt/hadoop/bin/hdfs dfs -chmod 777 /test
    echo "$expected_text" | docker exec -i -u hdfs hdfs /opt/hadoop/bin/hdfs dfs -put - "/$preexist_file"
    docker exec -u hdfs hdfs /opt/hadoop/bin/hdfs dfs -chmod 644 "/$preexist_file"
    docker exec -u hdfs hdfs /opt/hadoop/bin/hdfs dfs -ls /test/

    # 3. Stage Hadoop config for hdfs-native (libhdfs). Without
    #    HADOOP_CONF_DIR it falls back to defaults that don't
    #    include the test realm's KDC.
    mkdir -p /tmp/hadoop-conf
    cp /tmp/core-site.xml /tmp/hadoop-conf/
    cp /tmp/hdfs-site.xml /tmp/hadoop-conf/

    # 4. Mount via the hdfs hostname (not localhost) so the GSSAPI
    #    service principal matches what the hdfs server registered
    #    under at kadmin time. KRB5CCNAME propagates the host's
    #    ticket cache into the daemon child (which has no env except
    #    what we explicitly set).
    sudo mkdir -p "$mount_path" && sudo chmod 777 "$mount_path"
    local KRB5CCNAME
    KRB5CCNAME=$(klist 2>/dev/null | sed -n 's/^Ticket cache: //p')
    HADOOP_CONF_DIR=/tmp/hadoop-conf \
    KRB5_CONFIG=/etc/krb5.conf \
    KRB5CCNAME="${KRB5CCNAME}" \
        MNTRS_DAEMON_LOG="$log_path" \
        ./target/release/mntrs mount "hdfs://${hdfs_hostname}:8020/" "$mount_path" \
            --allow-other --daemon --daemon-wait --daemon-timeout=20 \
            --opt "dfs.namenode.kerberos.principal=${principal}" \
            > "$log_path" 2>&1 &

    # 5. Readiness probe — `cat` not `ls` because hdfs-native list_op
    #    trips FUSE readdir EIO during NN startup; cat uses read
    #    which is the stable path.
    local READY=0
    local i
    for i in $(seq 1 60); do
        if mount | grep -q "$mount_path" && cat "$mount_path/$preexist_file" >/dev/null 2>&1; then
            echo "mount ready after ${i}s"
            READY=1
            break
        fi
        sleep 1
    done

    if [ $READY -eq 0 ]; then
        echo "::error::HDFS Kerberos mount not ready after 60s"
        echo "--- mount log ---"
        cat "$log_path" 2>/dev/null || true
        echo "--- mount table ---"
        mount | grep hdfs || echo "(no hdfs mount)"
        echo "--- ps mntrs ---"
        ps aux | grep mntrs | grep -v grep || echo "(no mntrs process)"
        return 1
    fi

    local FAIL=0

    echo "--- 1. ls ---"
    ls -laR "$mount_path/" 2>&1

    echo "--- 2. read pre-existing ---"
    local GOT
    GOT=$(cat "$mount_path/$preexist_file" 2>/dev/null)
    if [ "$GOT" = "$expected_text" ]; then
        echo "read pre-existing OK: $preexist_file"
    else
        echo "::error::read pre-existing FAIL: $preexist_file (got '$GOT')"
        FAIL=1
    fi

    echo "--- 3. write small ---"
    if echo "hello kerberos" > "$mount_path/_ci_small.txt" 2>/dev/null; then
        echo "write OK"
    else
        echo "write FAIL"
        FAIL=1
    fi

    echo "--- 4. read back ---"
    GOT=$(cat "$mount_path/_ci_small.txt" 2>/dev/null)
    if [ "$GOT" = "hello kerberos" ]; then
        echo "read back OK"
    else
        echo "read back FAIL: got '$GOT'"
        FAIL=1
    fi

    echo "--- 5. append + verify ---"
    if echo "more" >> "$mount_path/_ci_small.txt" 2>/dev/null; then
        echo "append OK"
    else
        echo "append FAIL"
        FAIL=1
    fi
    GOT=$(cat "$mount_path/_ci_small.txt" 2>/dev/null)
    local EXPECTED
    EXPECTED=$(printf "hello kerberos\nmore")
    if [ "$GOT" = "$EXPECTED" ]; then
        echo "append verify OK"
    else
        echo "::error::append verify FAIL: got '$GOT'"
        FAIL=1
    fi

    echo "--- 6. append pre-existing ---"
    if echo "appended" >> "$mount_path/$preexist_file" 2>/dev/null; then
        echo "append pre-existing OK"
    else
        echo "append pre-existing FAIL"
        FAIL=1
    fi
    GOT=$(cat "$mount_path/$preexist_file" 2>/dev/null)
    if echo "$GOT" | grep -q "appended"; then
        echo "append pre-existing verify OK"
    else
        echo "::error::append pre-existing verify FAIL: got '$GOT'"
        FAIL=1
    fi

    echo "--- 7. write+read 10M ---"
    if dd if=/dev/urandom of="$mount_path/_ci_10m.bin" bs=1M count=10 2>/dev/null; then
        echo "write 10M OK"
    else
        echo "write 10M FAIL"
        FAIL=1
    fi
    if dd if="$mount_path/_ci_10m.bin" of=/dev/null bs=64K 2>/dev/null; then
        echo "read 10M OK"
    else
        echo "read 10M FAIL"
        FAIL=1
    fi

    echo "--- 8. random seek ---"
    for off in 0 500 10000 50000 500000 5000000 9000000 9999999; do
        if dd if="$mount_path/_ci_10m.bin" bs=1 count=1 skip="$off" of=/dev/null 2>/dev/null; then
            echo "  seek $off OK"
        else
            echo "  seek $off FAIL"
            FAIL=1
        fi
    done

    echo "--- 9. delete + recreate ---"
    rm -f "$mount_path/_ci_small.txt" 2>/dev/null
    if echo "recreated" > "$mount_path/_ci_small.txt" 2>/dev/null; then
        echo "recreate OK"
        GOT=$(cat "$mount_path/_ci_small.txt" 2>/dev/null)
        if [ "$GOT" = "recreated" ]; then
            echo "recreate verify OK"
        else
            echo "::error::recreate verify FAIL: got '$GOT'"
            FAIL=1
        fi
    else
        echo "::error::recreate FAIL"
        FAIL=1
    fi

    echo "--- 10. dir create + list + delete ---"
    if mkdir -p "$mount_path/_ci_dir" 2>/dev/null; then
        echo "mkdir OK"
    else
        echo "mkdir FAIL"
        FAIL=1
    fi
    echo "dirfile" > "$mount_path/_ci_dir/file.txt" 2>/dev/null
    GOT=$(ls "$mount_path/_ci_dir/" 2>/dev/null)
    if [ "$GOT" = "file.txt" ]; then
        echo "dir list OK"
    else
        echo "::error::dir list FAIL: got '$GOT'"
        FAIL=1
    fi
    if rm -rf "$mount_path/_ci_dir" 2>/dev/null; then
        echo "rmdir OK"
    else
        echo "rmdir FAIL"
        FAIL=1
    fi

    # Clean up test files; leave the mount alive so failure
    # diagnostics in Cleanup step + mount log artifact still have
    # the FUSE session to inspect.
    rm -f "$mount_path/_ci_small.txt" "$mount_path/_ci_10m.bin" 2>/dev/null || true

    if [ $FAIL -eq 0 ]; then
        echo "✅ HDFS Kerberos mount OK"
        return 0
    else
        echo "::error::HDFS Kerberos mount tests FAILED"
        return 1
    fi
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    hdfs_kerberos_mount_test "$@"
fi