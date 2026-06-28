#!/usr/bin/env pwsh
# tests/e2e/common/mount-test.ps1
#
# Windows WinFSP equivalent of tests/e2e/common/mount-test.sh.
# Runs the same effective sub-test matrix that integration.yml runs
# for the FUSE path, adapted to WinFSP via PowerShell 7 .NET APIs.
#
# Eight sub-tests (mirrors mount-test.sh lines 136-211):
#   1. mount + readiness probe (60s budget)
#   2. ls (Get-ChildItem)
#   3. cat pre-existing (skipped for memory backend)
#   4. write small file
#   5. read back
#   6. append + verify
#   7. 10M write + read
#   8. random seek at 8 offsets
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
    [ValidateSet('auto', 'debug', 'release')] [string] $Profile = 'auto'
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
        [ValidateSet('auto', 'debug', 'release')] [string] $Profile = 'auto'
    )

    # Counter must be script-scoped AND pre-initialized. The Fail
    # function increments $script:fail++; reading $script:fail when
    # it is still $null returns $null, and `$null -eq 0` is False
    # in PowerShell (not True), so a "0-fail" run would otherwise
    # fall through to the "FAILED" summary branch.
    $script:fail = 0

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

    # --- 1. Mount ------------------------------------------------
    # WinFSP mount: the process stays in the foreground keep-alive
    # loop (mount.rs:1526-1536) on Windows. We background via
    # Start-Process + capture PID + log paths.
    Write-Host "--- 1. mount ---"
    $logErr = "$LogPath.err"
    $proc = Start-Process -FilePath $MntrsBin `
        -ArgumentList @('mount', $Storage, $MountPath) `
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
    if (-not [string]::IsNullOrEmpty($PreexistFile)) {
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
    try {
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
    }

    # --- 5. read back --------------------------------------------
    Write-Host "--- 5. read back ---"
    try {
        $got = (Get-Content -Path "$MountPath\_ci_small.txt" -Raw -ErrorAction Stop).TrimEnd("`n")
        $expected = "hello from $Backend"
        if ($got -eq $expected) {
            Pass "read back matches"
        } else {
            Fail "read back" "(got '$got')"
        }
    } catch {
        Fail "read back" $_.Exception.Message
    }

    # --- 6. append + verify --------------------------------------
    Write-Host "--- 6. append + verify ---"
    try {
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
    }

    # --- 7. 10M write + read -------------------------------------
    Write-Host "--- 7. 10M write + read ---"
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
    }

    # --- 8. random seek ------------------------------------------
    Write-Host "--- 8. random seek ---"
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

    # --- cleanup test files (mount stays alive) ------------------
    # Best-effort; the workflow's if: always() cleanup step handles
    # process kill + unmount. Use Test-Path first so we never call
    # Remove-Item on a non-existent path (which renders an error
    # in pwsh 7 even with -ErrorAction SilentlyContinue).
    foreach ($f in @("$MountPath\_ci_small.txt", $bigPath)) {
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