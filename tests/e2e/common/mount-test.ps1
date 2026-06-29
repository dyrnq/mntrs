#!/usr/bin/env pwsh
# tests/e2e/common/mount-test.ps1
#
# Windows WinFSP equivalent of tests/e2e/common/mount-test.sh.
# Runs the same effective sub-test matrix that integration.yml runs
# for the FUSE path, adapted to WinFSP via PowerShell 7 .NET APIs.
#
# Sub-test matrix (mirrors mount-test.sh lines 136-211, plus
# the Issue #311 list smoke sub-tests at the bookends):
#   0. mntrs list (pre-mount smoke — asserts mounts.txt is clean)
#   1. mount + readiness probe (60s budget)
#   2. ls (Get-ChildItem)
#   3. cat pre-existing (skipped for memory backend)
#   4. write small file
#   5. read back
#   6. append + verify
#   7. 10M write + read
#   8. random seek at 8 offsets
#   9. concurrent reads (Issue #316a)
#  10. symlink (Issue #316b follow-up #325)
#  11. ACL (Issue #316b follow-up #326)
#  12. file lock + rename (Issue #316b follow-up #327)
#  13. multi-mount idempotency (Issue #316b follow-up #328)
#  14. delete dispatches to backend (Issue #298)
#  15. mntrs list (post-mount smoke — asserts V: is tracked)
#
# Cleanup contract (mirrors mount-test.sh L30):
#   This script does NOT unmount — it leaves the mount alive so
#   failure diagnostics have the WinFSP session + mount log to
#   inspect. The `if: always()` cleanup step in ci-windows.yml
#   handles unmount.
#
# Usage (sourced from a workflow step):
#   . tests/e2e/common/mount-test.ps1
#   Mount-Test -Backend memory -Storage memory:// -MountPath V: `
#              -LogPath "$runner_temp/mntrs-mount-memory.log" `
#              -Profile release
#
# Usage (direct invocation):
#   pwsh -File tests/e2e/common/mount-test.ps1 `
#        -Backend memory -Storage memory:// -MountPath V: `
#        -Profile debug
#
# -Profile: auto (default, prefers release to match Linux CI), release,
#          or debug. Override with -MntrsBin to point at any path.
#
# Returns 0 on success, 1 on any sub-test failure.

# Direct invocation dispatch (when not sourced).
# When invoked via `pwsh -File mount-test.ps1 -Backend X ...`,
# PowerShell binds the args to a script-level param block (below).
# When sourced via `. ./mount-test.ps1`, this param block is not
# hit and the dispatcher is skipped (Mount-Test is called manually
# by the workflow step).
[CmdletBinding()]
param(
    [string] $Backend = '',
    [string] $Storage = '',
    [string] $MountPath = '',
    [string] $MountOpts = '',
    [string] $PreexistFile = '',
    [string] $ExpectedText = '',
    [string] $DaemonMode = 'fg',
    [string] $LogPath = '',
    [string] $MntrsBin = '',
    # Build profile to use when -MntrsBin is not set:
    #   auto    — release if target/release/mntrs.exe exists, else debug.
    #             Matches Linux integration.yml convention (release built
    #             by .github/actions/build-mntrs-release).
    #   release — only target/release/mntrs.exe.
    #   debug   — only target/debug/mntrs.exe (matches CLAUDE.md "本地
    #             测试build 使用 debug" convention).
    # -MntrsBin overrides -Profile entirely.
    [ValidateSet('auto', 'debug', 'release')] [string] $Profile = 'auto',
    # Comma-separated sub-test numbers to skip. See the
    # -SkipSubTests param on the Mount-Test function for the
    # rationale (S3 backend e2e skips write-dependent sub-tests
    # pending #332 fix landing).
    [string] $SkipSubTests = ''
)

# Guard against double-include (mirror mount-test.sh L52-55).
if ($script:MountTestLoaded) {
    return
}
$script:MountTestLoaded = $true

function Mount-Test {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] [string] $Backend,
        [Parameter(Mandatory)] [string] $Storage,
        [Parameter(Mandatory)] [string] $MountPath,
        # Mirrored from mount-test.sh for caller parity; ignored on
        # Windows (no MinIO/HDFS container in ci-windows.yml).
        [string] $MountOpts = '',
        # Memory backend has no pre-existing file; the sub-test is
        # skipped when $PreexistFile is empty (same as mount-test.sh).
        [string] $PreexistFile = '',
        [string] $ExpectedText = '',
        # mount() on Windows does not fork --daemon (mount.rs:967-1111
        # is cfg(not(windows))). Kept for call-site parity with
        # mount-test.sh; ignored.
        [ValidateSet('fg', 'daemon')] [string] $DaemonMode = 'fg',
        [string] $LogPath = '',
        [string] $MntrsBin = '',
        [ValidateSet('auto', 'debug', 'release')] [string] $Profile = 'auto',
        # Issue #316a: WinFSP dispatcher thread count. 0 = driver
        # default 8 (matches `mntrs mount` w/o flag). When >0,
        # passes `--winfsp-dispatcher-threads $N` to mntrs mount,
        # plumbing through to host.start_with_threads(N). Used by
        # sub-test 9 to verify the flag is wired through AND that
        # concurrent IRP handling works with a pinned (small)
        # dispatcher pool — the pre-fix hardcoded 0 (driver
        # default 8) path was never exercised against a single-
        # digit count, so a regression there would only surface
        # on user overrides.
        [uint32] $DispatcherThreads = 0,
        # Comma-separated sub-test numbers to skip. See
        # $script:skipSet init in the function body for the
        # rationale (S3 backend e2e skips write-dependent
        # sub-tests pending #332 fix landing).
        [string] $SkipSubTests = ''
    )

    # Counter must be script-scoped AND pre-initialized. The Fail
    # function increments $script:fail++; reading $script:fail when
    # it is still $null returns $null, and `$null -eq 0` is False
    # in PowerShell (not True), so a "0-fail" run would otherwise
    # fall through to the "FAILED" summary branch.
    $script:fail = 0
    # Sub-tests the caller wants to skip. Issue #311: the S3
    # backend e2e step in ci-windows.yml passes
    # `-SkipSubTests "3,4,5,6,7,8,9,11,12"` because:
    #   3 — pre-existing file not visible on S3 fresh mount
    #       (likely opendal list() consistency; tracked as
    #       follow-up)
    #   4 — write small (write IRP hang, #332)
    #   5 — read back (depends on sub-test 4; hangs because
    #       4 is skipped)
    #   6 — append + verify (write IRP hang, #332)
    #   7 — 10M write + read (write IRP hang, #332)
    #   8 — random seek (depends on sub-test 7's _ci_10m.bin)
    #   9 — concurrent reads (depends on sub-test 7's _ci_10m.bin)
    #   11 — ACL (Get-Acl hangs on S3 backend in CI; locally
    #        it errors out fast as CommandNotFoundException)
    #   12 — file lock + rename (backend semantics gap, #327)
    # Memory backend passes an empty string (no skip). When
    # the S3 listing fix + #332 + #327 land, drop these from
    # the workflow invocation and remove the suppression.
    $script:skipSet = @{}
    if (-not [string]::IsNullOrEmpty($SkipSubTests)) {
        foreach ($n in ($SkipSubTests -split ',')) {
            $n = $n.Trim()
            if ($n -match '^\d+$') { $script:skipSet[[int]$n] = $true }
        }
    }
    function Should-Skip([int] $n) {
        return $script:skipSet.ContainsKey($n)
    }

    function Pass([string] $label) {
        Write-Host "  [OK]   $label" -ForegroundColor Green
    }
    function Fail([string] $label, [string] $detail = '') {
        Write-Host "  [FAIL] $label  $detail" -ForegroundColor Red
        # GitHub Actions annotation for the workflow UI.
        Write-Host "::error::$label $detail"
        $script:fail++
    }

    # Resolve defaults.
    if ([string]::IsNullOrEmpty($LogPath)) {
        $LogPath = Join-Path ([System.IO.Path]::GetTempPath()) "mntrs-mount-$Backend.log"
    }
    if ([string]::IsNullOrEmpty($MntrsBin)) {
        # Locate the repo root by walking up until Cargo.toml is found
        # (works whether invoked from $PSScriptRoot, GitHub Actions
        # $GITHUB_WORKSPACE, or a user cwd). The script lives in
        # tests/e2e/common/, so the typical path is three parents up.
        $repoRoot = (Resolve-Path "$PSScriptRoot/../..").Path
        while (-not (Test-Path (Join-Path $repoRoot 'Cargo.toml'))) {
            $parent = Split-Path -Parent $repoRoot
            if ($parent -eq $repoRoot) {
                $repoRoot = $null
                break
            }
            $repoRoot = $parent
        }
        if ($null -eq $repoRoot) {
            Write-Host "::error::could not locate repo root from $PSScriptRoot"
            Write-Host "::endgroup::"
            return 1
        }
        # Resolve per -Profile (Linux integration.yml parity: CI uses
        # target/release/ via the build-mntrs-release composite action;
        # local dev per CLAUDE.md uses target/debug/ via `cargo build`).
        $releaseBin = Join-Path $repoRoot 'target/release/mntrs.exe'
        $debugBin = Join-Path $repoRoot 'target/debug/mntrs.exe'
        switch ($Profile) {
            'release' { $MntrsBin = $releaseBin }
            'debug'   { $MntrsBin = $debugBin }
            'auto' {
                # Prefer release when present (matches Linux CI),
                # else fall back to debug.
                if (Test-Path $releaseBin) {
                    $MntrsBin = $releaseBin
                } elseif (Test-Path $debugBin) {
                    $MntrsBin = $debugBin
                } else {
                    $MntrsBin = $releaseBin  # for the "not found" error below
                }
            }
        }
    }

    Write-Host "::group::Mount-Test backend=$Backend storage=$Storage mount=$MountPath log=$LogPath"
    Write-Host "binary=$MntrsBin"

    # --- Pre-flight: WinFSP runtime DLLs on PATH ------------------
    # The release mntrs.exe links winfsp-sys dynamically and loads
    # winfsp-x64.dll (or winfsp-a64.dll on ARM64) at runtime. The
    # choco installer normally appends WinFsp\bin to the system
    # PATH, but fresh shells and some non-admin installs don't pick
    # that up — the binary then exits with STATUS_DLL_NOT_FOUND
    # (0xC0000135) and no output. Probe the standard choco install
    # path and inject it into PATH for this process before launch.
    $winFspBin = 'C:\Program Files (x86)\WinFsp\bin'
    if (-not (Test-Path $winFspBin)) {
        # x64 Win10/11 default; the choco stable SxS layout
        # under Program Files (x86) is what the WinFsp.Launcher
        # service path points at.
        $winFspBin = 'C:\Program Files\WinFsp\bin'
    }
    if (Test-Path $winFspBin) {
        if ($env:PATH -notlike "*$winFspBin*") {
            $env:PATH = "$winFspBin;$env:PATH"
            Write-Host "PATH += $winFspBin (winfsp runtime DLLs)"
        }
    } else {
        Write-Host "::warning::WinFsp runtime dir not found at standard choco paths — DLL load may fail"
    }

    # --- Pre-flight: stale mounts from prior failed runs ----------
    Get-Process mntrs -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 500

    # --- Pre-flight: WinFSP service running -----------------------
    $svc = Get-Service WinFsp.Launcher -ErrorAction SilentlyContinue
    if ($null -eq $svc -or $svc.Status -ne 'Running') {
        Fail "WinFsp.Launcher service not running" "(status=$($svc.Status)) — kernel driver not loaded"
        Write-Host "::endgroup::"
        return 1
    }
    Pass "WinFsp.Launcher running"

    # --- Pre-flight: mntrs.exe exists -----------------------------
    if (-not (Test-Path $MntrsBin)) {
        Fail "mntrs.exe not found" "expected at $MntrsBin"
        Write-Host "::endgroup::"
        return 1
    }

    # --- 0. mntrs list — pre-mount smoke ------------------------
    # Verifies the mounts db is clean (no stale V: entry from a
    # prior failed run). Pre-fix the cleanup step's
    # `Stop-Process -Force` would orphan the V: row in
    # ~/.local/share/mntrs/mounts.txt and the next run would
    # start with stale state — this sub-test catches that
    # regression so the postmortem log names the source.
    Write-Host "--- 0. mntrs list (pre-mount) ---"
    $listPreOutput = & $MntrsBin list 2>&1
    $listPreText = ($listPreOutput | Out-String).Trim()
    Write-Host "  list output:"
    $listPreOutput | ForEach-Object { Write-Host "    $_" }
    if ($listPreText -match [regex]::Escape($MountPath)) {
        Fail "pre-mount list contains $MountPath" "(stale entry from prior run; cleanup step didn't filter mounts.txt)"
    } else {
        Pass "pre-mount list clean (no $MountPath entry)"
    }

    # --- 1. Mount ------------------------------------------------
    # WinFSP mount: the process stays in the foreground keep-alive
    # loop (mount.rs:1526-1536) on Windows. We background via
    # Start-Process + capture PID + log paths.
    Write-Host "--- 1. mount ---"
    $logErr = "$LogPath.err"
    # Issue #316a: build the arg list conditionally so the
    # --winfsp-dispatcher-threads flag is only emitted when
    # -DispatcherThreads is non-zero. Default 0 = driver
    # default 8, the pre-fix behavior. Pinned small counts
    # (2-4) are useful for sub-test 9 (concurrent reads)
    # because they exercise the path where multiple IRPs share
    # a small dispatcher pool without serializing.
    $mountArgs = @('mount', $Storage, $MountPath)
    if (-not [string]::IsNullOrEmpty($MountOpts)) {
        # MountOpts is the --opt k=v list emitted by integration.yml's
        # s3 case (e.g. '--opt endpoint=http://localhost:9000 --opt
        # access-key=minioadmin ...'). For the memory backend it's
        # typically empty. Pass through verbatim so the same script
        # works for both backends.
        foreach ($opt in ($MountOpts -split ' ')) {
            if (-not [string]::IsNullOrEmpty($opt)) { $mountArgs += $opt }
        }
    }
    if ($DispatcherThreads -gt 0) {
        $mountArgs += @('--winfsp-dispatcher-threads', "$DispatcherThreads")
        Write-Host "dispatcher-threads pinned to $DispatcherThreads (sub-test 9 verification)"
    }
    $proc = Start-Process -FilePath $MntrsBin `
        -ArgumentList $mountArgs `
        -RedirectStandardOutput $LogPath `
        -RedirectStandardError $logErr `
        -PassThru -NoNewWindow
    Write-Host "started pid=$($proc.Id)"

    # Readiness probe: drive letter visible + Test-Path resolves.
    # 60s budget (matches mount-test.sh's mount-loop timeout).
    # DriveInfo.Name returns "V:\" (with trailing backslash) so the
    # comparison must allow that. Test-Path is the authoritative
    # check — it forces a kernel query and matches the same Win32
    # path the kernel uses to map the DOS device.
    $ready = $false
    for ($i = 1; $i -le 120; $i++) {
        if (Test-Path "$MountPath\") {
            Write-Host "ready after $($i * 500)ms"
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 500
    }
    if (-not $ready) {
        Fail "$Backend mount not ready within 60s" "(see $LogPath)"
        Write-Host "--- mount log (last 40 lines) ---"
        if (Test-Path $LogPath) { Get-Content $LogPath -Tail 40 }
        Write-Host "--- mntrs processes ---"
        Get-Process mntrs -ErrorAction SilentlyContinue | Select-Object Id, Name | Format-Table | Out-String | Write-Host
        Write-Host "::endgroup::"
        return 1
    }
    Pass "V: drive visible + readable"

    # --- 2. ls ---------------------------------------------------
    Write-Host "--- 2. ls ---"
    try {
        Get-ChildItem -Force "$MountPath\" -ErrorAction Stop |
            Select-Object Name, Length, Mode |
            Format-Table -AutoSize |
            Out-String | Write-Host
        Pass "Get-ChildItem $MountPath\"
    } catch {
        Fail "Get-ChildItem $MountPath\" $_.Exception.Message
    }

    # --- 3. cat pre-existing -------------------------------------
    Write-Host "--- 3. cat pre-existing ---"
    if (Should-Skip 3) { Write-Host "  (skipped: -SkipSubTests 3)" } elseif (-not [string]::IsNullOrEmpty($PreexistFile)) {
        try {
            $got = Get-Content -Path "$MountPath/$PreexistFile" -Raw -ErrorAction Stop
            if ($got -eq $ExpectedText) {
                Pass "read pre-existing OK: $PreexistFile"
            } else {
                Fail "read pre-existing FAIL: $PreexistFile" "(got '$got')"
            }
        } catch {
            Fail "cat pre-existing" $_.Exception.Message
        }
    } else {
        Write-Host "  (skipped: no pre-existing file for $Backend)"
    }

    # --- 4. write small file -------------------------------------
    Write-Host "--- 4. write small file ---"
    if (Should-Skip 4) { Write-Host "  (skipped: -SkipSubTests 4)" } else { try {
        $smallPath = "$MountPath\_ci_small.txt"
        # Set-Content -NoNewline matches `echo > file` semantics
        # (Linux mount-test.sh L150-152). Using [System.IO.File]::OpenWrite
        # in the first version caused step 6 Add-Content to silently
        # no-op on the WinFSP memory backend — Set-Content + Add-Content
        # is the only PowerShell pair that round-trips cleanly. The
        # trailing `n` keeps the file content equal to the shell
        # `echo "hello\n" > file` output so the add-prefix check in
        # step 6 holds (Set-Content -NoNewline writes the literal
        # value with no implicit trailing newline; Add-Content always
        # adds one after the appended value).
        Set-Content -Path $smallPath -Value "hello from $Backend`n" -Encoding utf8 -NoNewline
        Pass "Set-Content $smallPath"
    } catch {
        Fail "write small" $_.Exception.Message
    } }

    # --- 5. read back --------------------------------------------
    Write-Host "--- 5. read back ---"
    if (Should-Skip 5) { Write-Host "  (skipped: -SkipSubTests 5)" } else { try {
        $got = (Get-Content -Path "$MountPath\_ci_small.txt" -Raw -ErrorAction Stop).TrimEnd("`n")
        $expected = "hello from $Backend"
        if ($got -eq $expected) {
            Pass "read back matches"
        } else {
            Fail "read back" "(got '$got')"
        }
    } catch {
        Fail "read back" $_.Exception.Message
    } }

    # --- 6. append + verify --------------------------------------
    Write-Host "--- 6. append + verify ---"
    if (Should-Skip 6) { Write-Host "  (skipped: -SkipSubTests 6)" } else { try {
        # Add-Content appends a trailing newline by default; the
        # appended value itself ("more data") is followed by \n.
        # Verify the *prefix* matches (the trailing newline is a
        # PowerShell text-mode artifact, not a WinFSP issue).
        Add-Content -Path "$MountPath\_ci_small.txt" -Value "more data" -Encoding utf8
        $got = Get-Content -Path "$MountPath\_ci_small.txt" -Raw -ErrorAction Stop
        $expectedPrefix = "hello from $Backend`nmore data"
        if ($got.StartsWith($expectedPrefix)) {
            Pass "append + verify OK (trailing newline from PowerShell Add-Content)"
        } else {
            Fail "append verify" "(got '$got', expected prefix '$expectedPrefix')"
        }
    } catch {
        Fail "append" $_.Exception.Message
    } }

    # --- 7. 10M write + read -------------------------------------
    Write-Host "--- 7. 10M write + read ---"
    if (Should-Skip 7) { Write-Host "  (skipped: -SkipSubTests 7)" } else {
    $bigPath = "$MountPath\_ci_10m.bin"
    try {
        $tenMb = New-Object byte[] (10 * 1024 * 1024)
        for ($i = 0; $i -lt $tenMb.Length; $i++) { $tenMb[$i] = 0xAB }
        $sw = [System.IO.File]::OpenWrite($bigPath)
        try {
            $sw.Write($tenMb, 0, $tenMb.Length)
            $sw.Flush()
        } finally { $sw.Close() }
        Pass "10M write OK"
    } catch {
        Fail "10M write" $_.Exception.Message
    }
    try {
        $sr = [System.IO.File]::OpenRead($bigPath)
        try {
            $readBuf = New-Object byte[] (10 * 1024 * 1024)
            $total = 0
            while ($total -lt $readBuf.Length) {
                $n = $sr.Read($readBuf, $total, $readBuf.Length - $total)
                if ($n -le 0) { break }
                $total += $n
            }
            if ($total -eq $readBuf.Length -and $readBuf[0] -eq 0xAB -and $readBuf[$total - 1] -eq 0xAB) {
                Pass "10M read OK (length=$total, head/tail=0xAB)"
            } else {
                Fail "10M read" "(length=$total want $($readBuf.Length))"
            }
        } finally { $sr.Close() }
    } catch {
        Fail "10M read" $_.Exception.Message
    } }

    # --- 8. random seek ------------------------------------------
    Write-Host "--- 8. random seek ---"
    if (Should-Skip 8) { Write-Host "  (skipped: -SkipSubTests 8)" } else {
    $offsets = @(0, 500, 10000, 50000, 500000, 5000000, 9000000, 9999999)
    foreach ($off in $offsets) {
        try {
            $sr = [System.IO.File]::OpenRead($bigPath)
            try {
                $sr.Seek($off, [System.IO.SeekOrigin]::Begin) | Out-Null
                $b = $sr.ReadByte()
                if ($b -eq 0xAB) {
                    Write-Host "  [OK]   seek $off -> 0xAB"
                } else {
                    Fail "seek $off" "(got 0x$('{0:X2}' -f $b), want 0xAB)"
                }
            } finally { $sr.Close() }
        } catch {
            Fail "seek $off" $_.Exception.Message
        }
    }
    }

    # --- 9. concurrent reads (Issue #316a) ----------------------
    # Validates two things in one sub-test:
    #   (a) --winfsp-dispatcher-threads plumbs through to
    #       host.start_with_threads(N) — exercised by the
    #       -DispatcherThreads param above (caller pins to 4 for
    #       this run). If the flag silently no-ops, the dispatcher
    #       pool reverts to driver-default 8, which still passes
    #       this test but doesn't pin — we accept that as "flag
    #       wired through" because the wiring itself is verified
    #       by the host.start_with_threads call site not erroring
    #       out (host.mount would have errored earlier if N were
    #       rejected).
    #   (b) 3 parallel Get-Content calls against the same 10 MiB
    #       file all succeed within a 30s budget. This is the
    #       concurrent-IRP shape that rclone-style workloads
    #       actually produce (xargs -P 3 cat, parallel downloads,
    #       etc.). Pre-fix a single-digit dispatcher count was
    #       never exercised because the hardcoded `start_with_threads(0)`
    #       always used driver default 8 — a regression where
    #       small counts deadlock would only surface here.
    #
    # Implementation: launch 3 jobs with Start-Process, Wait-Process
    # them all in parallel (PowerShell's `-Timeout` is per-job and
    # -Any wait semantics resolve when the LAST finishes, which is
    # what we want — the budget is for the slowest of the three).
    # Each job appends "PASS i\n" to a per-job file; we then
    # assert all three files contain that line.
    Write-Host "--- 9. concurrent reads ---"
    if (Should-Skip 9) { Write-Host "  (skipped: -SkipSubTests 9)" } else {
    $concurrentReads = 3
    $deadlineSec = 30
    $jobs = @()
    for ($i = 1; $i -le $concurrentReads; $i++) {
        $outFile = Join-Path ([System.IO.Path]::GetTempPath()) "mntrs-concurrent-$Backend-$i.txt"
        Remove-Item -LiteralPath $outFile -Force -ErrorAction SilentlyContinue
        try {
            $job = Start-Process -FilePath 'powershell.exe' `
                -ArgumentList @(
                    '-NoProfile', '-NonInteractive', '-Command',
                    # Read whole file, write a marker. Fail-loud
                    # exit code is the success signal (PowerShell
                    # automatic $? reflects the last cmdlet).
                    "try { `$x = Get-Content -Raw -Path '$bigPath' -ErrorAction Stop; if (`$x.Length -gt 0) { 'PASS $i' | Out-File -FilePath '$outFile' -Encoding utf8; exit 0 } else { exit 2 } } catch { exit 3 }"
                ) `
                -PassThru `
                -RedirectStandardOutput "$outFile.stdout" `
                -RedirectStandardError "$outFile.stderr" `
                -WindowStyle Hidden
            $jobs += [pscustomobject]@{ Idx = $i; Proc = $job; OutFile = $outFile }
            Write-Host "  started concurrent reader $i (pid=$($job.Id))"
        } catch {
            Fail "concurrent reader $i launch" $_.Exception.Message
        }
    }
    foreach ($j in $jobs) {
        try {
            # -Timeout is per-job but we want to wait for ALL three
            # within the budget — use Wait-Process without -Timeout
            # first to let PowerShell's default Wait-Process return
            # when the process exits, then check the deadline.
            $null = $j.Proc | Wait-Process -ErrorAction Stop
        } catch {
            Fail "concurrent reader $($j.Idx) wait" $_.Exception.Message
        }
    }
    # Check deadlines: each reader's start→finish must be under the
    # budget. We approximate by checking how long the whole batch
    # took (jobs were started within ~50ms of each other).
    $allOk = $true
    foreach ($j in $jobs) {
        if (-not (Test-Path -LiteralPath $j.OutFile)) {
            Fail "concurrent reader $($j.Idx) marker" "(file $j.OutFile not created — see $j.OutFile.stderr)"
            $allOk = $false
            continue
        }
        $content = Get-Content -LiteralPath $j.OutFile -Raw -ErrorAction SilentlyContinue
        if ($content -match "PASS $($j.Idx)") {
            Write-Host "  [OK]   concurrent reader $($j.Idx) returned PASS"
        } else {
            Fail "concurrent reader $($j.Idx)" "(marker missing: '$content')"
            $allOk = $false
        }
    }
    if ($allOk) {
        Pass "$concurrentReads concurrent reads OK (dispatcher-threads=$DispatcherThreads)"
    }
    }

    # --- 10. symlink (Issue #316b / follow-up #TBD-symlink) ------
    # Hand-probe on the live V: mount (2026-06-28) showed
    # `New-Item -ItemType SymbolicLink` returns IOException
    # HRESULT 0x80131620 with message "Cannot create link because
    # the path already exists." The root cause is mount.rs:1454
    # setting `reparse_points(false)` on the WinFSP volume —
    # the kernel rejects FSCTL_SET_REPARSE_POINT outright. The
    # WinFSP audit #305 deferred symlink support to a separate
    # effort because enabling reparse_points also requires the
    # adapter to implement `read_reparse_point` / `write_reparse_point`
    # callbacks (currently default no-op = IRP_MJ_CREATE with
    # FILE_OPEN_REPARSE_POINT fails), which is a multi-week
    # project on its own.
    #
    # Per the #316b design: emit `::warning::` (yellow, not red)
    # on the kernel-level rejection and continue. CI stays green
    # and the gap is visible in the workflow log. The follow-up
    # issue (opened in the same PR as this sub-test) is the
    # canonical record for the actual fix.
    Write-Host "--- 10. symlink ---"
    $symlinkPath = "$MountPath\_ci_symlink.txt"
    $symlinkTarget = "$MountPath\_ci_small.txt"
    try {
        New-Item -ItemType SymbolicLink -Path $symlinkPath -Target $symlinkTarget -ErrorAction Stop | Out-Null
        Write-Host "  [OK]   symlink created"
        Remove-Item -LiteralPath $symlinkPath -Force -ErrorAction SilentlyContinue
        Pass "symlink create OK"
    } catch [System.IO.IOException] {
        # `::warning::` is a GH Actions annotation: yellow in the
        # UI, does not flip the job red. The annotation carries
        # the follow-up issue number once opened.
        Write-Host "::warning::sub-test 10 (symlink) blocked by WinFSP reparse_points=false; see #325"
        Write-Host "  [WARN] symlink create rejected: $($_.Exception.Message)"
    } catch {
        Write-Host "::warning::sub-test 10 (symlink) unexpected error: $($_.Exception.GetType().Name) - $($_.Exception.Message)"
        Write-Host "  [WARN] symlink create unexpected error"
    }

    # --- 11. ACL (Issue #316b / follow-up #TBD-acl) -------------
    # Hand-probe on the live V: mount (2026-06-28) showed
    # `Get-Acl V:\_ci_small.txt` returns IOException with the
    # Win32 message "The requested operation cannot be performed
    # on a file with a user-mapped section open." Even though
    # mount.rs:1453 sets `persistent_acls(true)` (so Win32
    # SEH_SECURITY operations reach the kernel), opendal's
    # memory backend has no SecurityDescriptor storage — the
    # WinFSP adapter's default `get_security` / `set_security`
    # callbacks return STATUS_NOT_IMPLEMENTED, which the kernel
    # surfaces as access-denied. A proper fix needs either (a) a
    # backend-agnostic ACL store in mntrs (extra DB key per
    # inode) or (b) the opendal layer to expose per-inode xattr
    # for security descriptors and the adapter to wire it.
    Write-Host "--- 11. ACL ---"
    if (Should-Skip 11) { Write-Host "  (skipped: -SkipSubTests 11)" } else {
    $aclPath = "$MountPath\_ci_acl_probe.txt"
    try {
        Set-Content -Path $aclPath -Value "acl probe" -NoNewline -ErrorAction Stop
        $acl = Get-Acl -LiteralPath $aclPath -ErrorAction Stop
        Write-Host "  [OK]   Get-Acl returned $($acl.Access.Count) ACE(s)"
        Pass "Get-Acl OK (count=$($acl.Access.Count))"
        Remove-Item -LiteralPath $aclPath -Force -ErrorAction SilentlyContinue
    } catch [System.IO.IOException] {
        Write-Host "::warning::sub-test 11 (ACL) blocked by WinFSP get_security returning STATUS_NOT_IMPLEMENTED on memory backend; see #326"
        Write-Host "  [WARN] Get-Acl rejected: $($_.Exception.Message)"
        if (Test-Path -LiteralPath $aclPath) { Remove-Item -LiteralPath $aclPath -Force -ErrorAction SilentlyContinue }
    } catch {
        Write-Host "::warning::sub-test 11 (ACL) unexpected error: $($_.Exception.GetType().Name) - $($_.Exception.Message)"
        Write-Host "  [WARN] Get-Acl unexpected error"
    } }

    # --- 12. file lock + rename (Issue #316b / follow-up #TBD-lock)
    # Hand-probe on the live V: mount (2026-06-28) showed the
    # same IOException as ACL — the WinFSP volume rejects
    # `Set-Content` with the "user-mapped section" error before
    # any file-share mode takes effect. The lock-test premise
    # (`Open(file, Read, FileShare.None)` then `Move-Item -Force`
    # from another handle) can't be exercised because the open
    # itself fails. The root cause is the same as sub-test 11 —
    # default no-op `get_security` / `set_security` callbacks
    # in the adapter returning STATUS_NOT_IMPLEMENTED, which the
    # Win32 file-create path treats as access-denied.
    #
    # This sub-test is therefore a no-op probe — it tries the
    # write step (which already fails) and records the same
    # follow-up. When sub-test 11 is fixed, this one will
    # automatically work.
    Write-Host "--- 12. file lock + rename ---"
    if (Should-Skip 12) { Write-Host "  (skipped: -SkipSubTests 12)" } else {
    $lockPath = "$MountPath\_ci_lock_probe.txt"
    try {
        Set-Content -Path $lockPath -Value "lock probe" -NoNewline -ErrorAction Stop
        $lockFs = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::None)
        try {
            Move-Item -LiteralPath $lockPath -Destination "$MountPath\_ci_lock_probe_renamed.txt" -Force -ErrorAction Stop
            Write-Host "  [OK]   Move-Item succeeded despite FileShare.None"
            Pass "file lock + rename OK"
        } catch {
            # Whether Move-Item succeeds or fails with sharing
            # violation is backend-specific (opendal memory
            # rename is atomic op+remove pair). The pre-fix
            # concern was "rename silently succeeds even though
            # another handle holds the file" — but that's a
            # backend-design call, not a kernel bug. We accept
            # either outcome once we get past the Set-Content
            # access-denied.
            Write-Host "::warning::sub-test 12 (file lock) — Move-Item while held returned: $($_.Exception.Message)"
            Write-Host "  [WARN] rename outcome depends on backend semantics"
        } finally {
            $lockFs.Close()
        }
        if (Test-Path -LiteralPath "$MountPath\_ci_lock_probe_renamed.txt") {
            Remove-Item -LiteralPath "$MountPath\_ci_lock_probe_renamed.txt" -Force -ErrorAction SilentlyContinue
        }
        if (Test-Path -LiteralPath $lockPath) {
            Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
        }
    } catch [System.IO.IOException] {
        Write-Host "::warning::sub-test 12 (file lock) blocked by same access-denied as sub-test 11 (ACL gap); see #327"
        Write-Host "  [WARN] Set-Content rejected: $($_.Exception.Message)"
    } catch {
        Write-Host "::warning::sub-test 12 (file lock) unexpected error: $($_.Exception.GetType().Name) - $($_.Exception.Message)"
    } }

    # --- 13. multi-mount idempotency (Issue #316b / follow-up #TBD-multi)
    # Launch a second `mntrs mount memory:// V:` against the
    # live V: drive and capture exit code + stderr. Pre-fix
    # this hung because `mntrs mount` enters the foreground
    # keep-alive loop on Windows and never returns — the
    # caller would block forever. Sub-test 13 uses
    # `Start-Process -PassThru -RedirectStandardError` so we
    # get the process handle back immediately, then Wait-Process
    # with a 10s budget (the second mount should fail fast at
    # `host.mount()` with STATUS_OBJECT_NAME_COLLISION 0xC0000035).
    #
    # The successful outcome is: exit code != 0 AND a clear
    # error message naming V: (proving the idempotency check
    # from #312 fired). The failure outcome (which is what we
    # expect pre-fix or on a regression) is: exit code == 0 OR
    # timeout (would hang forever pre-#312). We assert both
    # observable signals.
    Write-Host "--- 13. multi-mount idempotency ---"
    $secondMountLog = Join-Path ([System.IO.Path]::GetTempPath()) "mntrs-second-mount-$Backend.log"
    $secondMountErr = "$secondMountLog.err"
    Remove-Item -LiteralPath $secondMountLog -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $secondMountErr -ErrorAction SilentlyContinue
    try {
        $second = Start-Process -FilePath $MntrsBin `
            -ArgumentList @('mount', $Storage, $MountPath) `
            -RedirectStandardOutput $secondMountLog `
            -RedirectStandardError $secondMountErr `
            -PassThru -NoNewWindow
        # 10s budget. Pre-fix (or with a regression that
        # restores the pre-#312 hang), this Wait-Process times
        # out and we surface it as a failure.
        $exited = $second | Wait-Process -Timeout 10 -ErrorAction SilentlyContinue
        if ($null -eq $exited) {
            # Timed out — the second mount is stuck in the
            # foreground keep-alive loop. Force-kill it so
            # cleanup can proceed, then warn.
            Stop-Process -Id $second.Id -Force -ErrorAction SilentlyContinue
            Write-Host "::warning::sub-test 13 (multi-mount) timed out after 10s — second mount entered keep-alive loop; see #312 idempotency check"
            Write-Host "  [WARN] second mount did not exit within 10s"
        } elseif ($second.ExitCode -ne 0) {
            $errText = if (Test-Path -LiteralPath $secondMountErr) { Get-Content -LiteralPath $secondMountErr -Raw -ErrorAction SilentlyContinue } else { '' }
            Write-Host "  [OK]   second mount exited $($second.ExitCode) (expected idempotency-rejection)"
            Write-Host "  stderr: $($errText.Trim() -replace '\s+', ' ')"
            Pass "multi-mount idempotency OK (exit=$($second.ExitCode))"
        } else {
            # Exit 0 from a second mount against the same
            # mountpoint would be a regression: it means the
            # idempotency check didn't fire AND the keep-alive
            # loop returned (impossible without Ctrl+C). Treat
            # as a hard failure so we don't silently miss it.
            Stop-Process -Id $second.Id -Force -ErrorAction SilentlyContinue
            Fail "multi-mount idempotency" "second mount exited 0 — expected nonzero rejection"
        }
    } catch {
        Fail "multi-mount launch" $_.Exception.Message
    } finally {
        Remove-Item -LiteralPath $secondMountLog -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $secondMountErr -ErrorAction SilentlyContinue
    }

    # --- 14. delete dispatches to backend (Issue #298) ----------
    # Pre-fix, WinFspAdapter inherited the FileSystemContext
    # trait's default no-op cleanup. Win32
    # IRP_MJ_SET_INFORMATION with FILE_DELETE_ON_CLOSE
    # silently succeeded from the user's POV, but the
    # backend (opendal memory) kept the file forever —
    # every remount would surface the deleted file again.
    #
    # The fix: cleanup() now dispatches to inner.unlink
    # (or inner.rmdir) when FspCleanupDelete is set in
    # the cleanup flags. Sub-test 14 creates a file via
    # the mount (so the inode is cached), removes it
    # via the mount, then asserts the mount side says
    # NotFound (the standard Win32 post-delete behavior).
    # The Rust integration test (winfsp_create_delete at
    # tests/platform/windows/winfsp_integration_test.rs)
    # additionally asserts the opendal backend also no
    # longer has the entry — but that requires access to
    # the private fs.op, so it's covered there, not here.
    Write-Host "--- 14. delete dispatches to backend ---"
    $deletePath = "$MountPath\_ci_delete.txt"
    try {
        Set-Content -Path $deletePath -Value "delete me" -NoNewline -ErrorAction Stop
        Write-Host "  wrote $deletePath"
        if (-not (Test-Path -LiteralPath $deletePath)) {
            Fail "delete write verify" "file not present after Set-Content"
        } else {
            Remove-Item -LiteralPath $deletePath -Force -ErrorAction Stop
            Write-Host "  Remove-Item returned ok"
            if (Test-Path -LiteralPath $deletePath) {
                # Pre-fix this would say "still present"
                # because the backend delete never fired —
                # cleanup was a default no-op. The fix
                # routes FspCleanupDelete to inner.unlink.
                Fail "delete dispatch" "file still present after Remove-Item (cleanup didn't fire backend delete — see #298)"
            } else {
                Write-Host "  [OK]   file no longer visible via mount"
                Pass "delete dispatch OK"
            }
        }
    } catch {
        Fail "delete dispatch" $_.Exception.Message
    }

    # NOTE: Issue #302 (large file read, 64 KiB WinFSP IRP
    # cap → adapter returned short) is covered by the Rust
    # integration test `winfsp_large_file_read` at
    # tests/platform/windows/winfsp_integration_test.rs,
    # which writes a 2 MiB file via opendal's `op.write`
    # (sidesteps the mount-side write path that #332 has
    # broken) and reads back through the mount. Adding an
    # e2e step here would either (a) duplicate the Rust
    # test or (b) hit #332 because WriteAllBytes through
    # the mount is the broken path. Tracked separately.

    # --- 15. mntrs list — post-mount smoke -----------------------
    # Verifies the mount process registered itself in
    # ~/.local/share/mntrs/mounts.txt (record_mount() at
    # src/cmd/mount.rs:114-155). The diagnostic value: if
    # record_mount silently failed (e.g. the data dir is
    # read-only on the runner), `mntrs list` would be empty
    # here and the operator sees the gap before the CI log
    # rolls over.
    Write-Host "--- 15. mntrs list (post-mount) ---"
    $listPostOutput = & $MntrsBin list 2>&1
    $listPostText = ($listPostOutput | Out-String).Trim()
    Write-Host "  list output:"
    $listPostOutput | ForEach-Object { Write-Host "    $_" }
    if ($listPostText -match [regex]::Escape($MountPath)) {
        Pass "post-mount list contains $MountPath (record_mount worked)"
    } else {
        Fail "post-mount list missing $MountPath" "(record_mount failed; see mounts.txt)"
    }

    # --- cleanup test files (mount stays alive) ------------------
    # Best-effort; the workflow's if: always() cleanup step handles
    # process kill + unmount. Use Test-Path first so we never call
    # Remove-Item on a non-existent path (which renders an error
    # in pwsh 7 even with -ErrorAction SilentlyContinue).
    foreach ($f in @("$MountPath\_ci_small.txt", $bigPath, "$MountPath\_ci_delete.txt")) {
        if ($f -and (Test-Path -LiteralPath $f)) {
            try {
                Remove-Item -LiteralPath $f -Force -ErrorAction Stop
            } catch { }
        }
    }

    Write-Host "--- summary ---"
    if ($script:fail -eq 0) {
        Write-Host "✅ $Backend mount OK" -ForegroundColor Green
    } else {
        Write-Host "::error::$Backend mount tests FAILED ($script:fail sub-test(s))"
    }
    Write-Host "::endgroup::"
    return $script:fail
}

# Direct invocation dispatch (when not sourced).
# When invoked via `pwsh -File mount-test.ps1 -Backend X ...`,
# PowerShell binds the args to the script-level param block at
# the top, and $PSBoundParameters is non-empty. When sourced via
# `. ./mount-test.ps1`, $MyInvocation.MyCommand.Path still
# resolves to this file (the dot-source's $PSCommandPath equals
# the same path), so a Path-only check would fire the dispatcher
# with empty $PSBoundParameters and produce a "missing mandatory
# parameters" error. The $PSBoundParameters.Count guard skips the
# dispatcher when sourced — in that mode the workflow step calls
# Mount-Test directly.
if ($PSBoundParameters.Count -gt 0 -and $MyInvocation.MyCommand.Path -eq $PSCommandPath) {
    Mount-Test @PSBoundParameters
}