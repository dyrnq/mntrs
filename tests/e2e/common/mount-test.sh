#!/usr/bin/env bash
#
# tests/e2e/common/mount-test.sh
#
# Shared FUSE-mount smoke test for the three mntrs backends exercised
# by integration.yml (memory / s3 / hdfs) and csi-integration.yml
# (s3 / hdfs). Replaces two inline 97-190-line blocks that differed
# only in expected-text constants and step ordering.
#
# Eight sub-tests (see spec issue #167):
#   1. mount + readiness probe (60s)
#   2. ls
#   3. cat pre-existing (skipped for memory)
#   4. write small file
#   5. read back
#   6. append + verify
#   7. 10M write+read
#   8. random seek
#
# Caller-specific extras stay inline after mount_test returns:
#   - integration.yml adds: HDFS root preflight (already handled by
#     hdfs-prep.sh + the touch-based preflight inside this script),
#     append-pre-existing, recreate, directory create+list+delete.
#   - csi-integration.yml adds: CSI k3s smoke test (kubectl apply +
#     exec mntrs-csi --help).
#
# Both call sites retain their own cleanup_fuse_mount in Cleanup
# step (with `if: always()`); this script does NOT unmount — it
# leaves the mount alive so failure diagnostics have the FUSE
# session to inspect.
#
# Usage:
#   . tests/e2e/common/mount-test.sh
#   mount_test <backend> <storage> <mount_path> <mount_opts> \
#       <preexist_file> <expected_text> [daemon_mode] [log_path]
#
#   # defaults: daemon_mode=fg, log_path=/tmp/mntrs-mount-<backend>.log
#   # examples:
#   #   mount_test memory memory://           /mnt/mntrs-test \
#   #       "" "" "" "" ""
#   #   mount_test s3     s3://test-bucket    /mnt/mntrs-test \
#   #       "--opt endpoint=http://localhost:9000 ..." \
#   #       "s3-test.txt" "hello s3" "" ""
#   #   mount_test hdfs   hdfs://localhost:8020/ /mnt/mntrs-test \
#   #       "--opt dfs.client.use.datanode.hostname=true" \
#   #       "test/hello.txt" "hello hdfs" daemon ""
#
# Returns 0 if all sub-tests pass, 1 otherwise. Caller decides whether
# to `exit 1` on failure.

# Guard against double-include.
if [[ -n "${__MOUNT_TEST_LOADED:-}" ]]; then
    return 0 2>/dev/null || true
fi
__MOUNT_TEST_LOADED=1

mount_test() {
    local backend="$1"            # memory | s3 | hdfs
    local storage="$2"            # memory:// | s3://... | hdfs://...
    local mount_path="$3"         # /mnt/mntrs-test
    local mount_opts="$4"         # shell-quoted --opt ...
    local preexist_file="$5"      # s3-test.txt | test/hello.txt | ""
    local expected_text="$6"      # hello s3 | hello hdfs | ""
    local daemon_mode="${7:-fg}"  # fg | daemon
    local log_path="${8:-/tmp/mntrs-mount-${backend}.log}"

    sudo mkdir -p "$mount_path" && sudo chmod 777 "$mount_path"

    # 1. Mount. hdfs uses --daemon --daemon-wait (parent exits after
    #    spawning child); others run in foreground. MNTRS_DAEMON_LOG
    #    redirects the daemon child's stdout/stderr into log_path so
    #    the artifact isn't empty when the parent has already exited
    #    (integration + csi-integration both rely on log_path being
    #    populated for postmortem — see SESSION_PITFALLS §2.2).
    if [ "$daemon_mode" = "daemon" ]; then
        # shellcheck disable=SC2094  # intentional: parent + child write same log
        MNTRS_DAEMON_LOG="$log_path" \
            ./target/release/mntrs mount "$storage" "$mount_path" \
                $mount_opts --allow-other --daemon --daemon-wait \
                --daemon-timeout=20 > "$log_path" 2>&1 &
    else
        ./target/release/mntrs mount "$storage" "$mount_path" \
            $mount_opts > "$log_path" 2>&1 &
    fi

    # 2. Readiness probe — mount table + ls must both succeed.
    #    hdfs can take 30-60s for NN to settle; mem/s3 are sub-second.
    #    ls may transiently EIO on hdfs while NN finishes startup
    #    (SESSION_PITFALLS §2.4 — hdfs-native can return root entry
    #    / trailing slashes during startup).
    local READY=0
    local i
    for i in $(seq 1 60); do
        if mount | grep -q "$mount_path" && ls "$mount_path/" >/dev/null 2>&1; then
            echo "mount ready after ${i}s"
            READY=1
            break
        fi
        sleep 1
    done

    if [ $READY -eq 0 ]; then
        echo "::error::$backend mount not ready after 60s"
        echo "--- mount log ---"
        cat "$log_path" 2>/dev/null || true
        echo "--- mount table ---"
        mount | grep mntrs-test || echo "(no mount)"
        echo "--- mntrs processes ---"
        pgrep -alf mntrs || echo "(no mntrs process)"
        if [ "$backend" = "hdfs" ]; then
            echo "--- HDFS dfsadmin -report (server side) ---"
            docker exec -u hdfs hdfs /opt/hadoop/bin/hdfs dfsadmin -report 2>&1 \
                | tee /tmp/hdfs-dfsadmin-report.txt | head -30 || true
        fi
        echo "--- fusermount might be needed ---"
        return 1
    fi

    local FAIL=0

    # 3. HDFS preflight: the FUSE mount root maps to HDFS /, which
    #    the image boots as 755:hdfs:supergroup. Non-hdfs users can
    #    `ls` but not `create`. Catch the missing-chmod-777 class of
    #    bug up front instead of letting every later write fail with
    #    a cryptic AccessControlException.
    if [ "$backend" = "hdfs" ]; then
        if ! touch "$mount_path/.preflight_probe" 2>/dev/null; then
            echo "::error::HDFS root not writable by $(id -un) — fix: tests/e2e/common/hdfs-prep.sh docker (chmod 777 / + chmod 777 /test)"
            FAIL=1
        else
            rm -f "$mount_path/.preflight_probe"
            echo "preflight OK: mount root writable by $(id -un)"
        fi
    fi

    echo "--- 1. ls ---"
    ls -laR "$mount_path/" 2>&1

    echo "--- 2. cat pre-existing ---"
    if [ -n "$preexist_file" ]; then
        local GOT
        GOT=$(cat "$mount_path/$preexist_file" 2>/dev/null)
        if [ "$GOT" = "$expected_text" ]; then
            echo "read pre-existing OK: $preexist_file"
        else
            echo "::error::read pre-existing FAIL: $preexist_file (got '$GOT')"
            FAIL=1
        fi
    else
        echo "(skipped: no pre-existing file for $backend)"
    fi

    echo "--- 3. write small file ---"
    if echo "hello from $backend" > "$mount_path/_ci_small.txt" 2>/dev/null; then
        echo "write OK"
    else
        echo "write FAIL"
        FAIL=1
    fi

    echo "--- 4. read back written file ---"
    local GOT
    GOT=$(cat "$mount_path/_ci_small.txt" 2>/dev/null)
    if [ "$GOT" = "hello from $backend" ]; then
        echo "read back OK"
    else
        echo "::error::read back FAIL: got '$GOT'"
        FAIL=1
    fi

    echo "--- 5. append write + verify ---"
    if echo "more data" >> "$mount_path/_ci_small.txt" 2>/dev/null; then
        echo "append OK"
    else
        echo "append FAIL"
        FAIL=1
    fi
    GOT=$(cat "$mount_path/_ci_small.txt" 2>/dev/null)
    local EXPECTED
    EXPECTED=$(printf "hello from %s\nmore data" "$backend")
    if [ "$GOT" = "$EXPECTED" ]; then
        echo "append verify OK"
    else
        echo "::error::append verify FAIL: got '$GOT'"
        FAIL=1
    fi

    echo "--- 6. write+read 10M sequential ---"
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

    echo "--- 7. random read 10M ---"
    for off in 0 500 10000 50000 500000 5000000 9000000 9999999; do
        if dd if="$mount_path/_ci_10m.bin" bs=1 count=1 skip="$off" of=/dev/null 2>/dev/null; then
            echo "  seek $off OK"
        else
            echo "  seek $off FAIL"
            FAIL=1
        fi
    done

    # Clean up test files; leave the mount alive so failure
    # diagnostics (Cleanup step + mount log artifact) still have the
    # FUSE session to inspect.
    rm -f "$mount_path/_ci_small.txt" "$mount_path/_ci_10m.bin" 2>/dev/null || true

    if [ $FAIL -eq 0 ]; then
        echo "✅ $backend mount OK"
        return 0
    else
        echo "::error::$backend mount tests FAILED"
        return 1
    fi
}

# Direct invocation dispatch (when not sourced).
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
    mount_test "$@"
fi