# build-mntrs-release

Build the `mntrs` binary in release mode. Combines checkout,
rust-toolchain, rust-cache, the FUSE build deps, and
`cargo build --release -p mntrs` into a single action invocation.

## Inputs

| Input       | Default | Notes                                                                 |
|-------------|---------|-----------------------------------------------------------------------|
| `extra_deps`| `""`    | Extra apt packages. e.g. `krb5-user` for the hdfs-kerberos job.       |

## Replaces

- 5 inline "Build mntrs" steps in `integration.yml`
  (lines 49, 318, 375, 411, 460) — all currently
  `cargo build --release -p mntrs`.

## Example

```yaml
- uses: ./.github/actions/build-mntrs-release
```

```yaml
# hdfs-kerberos job also wants krb5-user installed:
- uses: ./.github/actions/build-mntrs-release
  with:
    extra_deps: krb5-user
```

Note: most callers run `install-linux-deps.sh` separately *before* this
action to install the same packages — that's fine, `apt-get install` is
idempotent. We could share the install, but keeping it explicit per job
makes the dependency surface obvious in the workflow file.
