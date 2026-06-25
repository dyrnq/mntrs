# setup-csi-image

Build the `mntrs-csi` binary, log in to GHCR, and push the CSI image.
Replaces the 4-step chain ("Install build deps" + "Build mntrs-csi" +
"Log in to GHCR" + "Build and push CSI image") used in csi-integration.yml
and csi-e2e.yml (s3, hdfs, hdfs-kerberos jobs).

## Inputs

| Input               | Default                              | Notes                                                          |
|---------------------|--------------------------------------|----------------------------------------------------------------|
| `target`            | `x86_64-unknown-linux-musl`          | Pass `x86_64-unknown-linux-gnu` for hdfs-kerberos.             |
| `image_tag`         | (required)                           | Full image tag — caller provides `ghcr.io/...`.                |
| `docker_context`    | `docker/csi`                         | Where the binary is staged before `docker build`.              |
| `install_deps`      | `musl-tools protobuf-compiler`       | Pass `protobuf-compiler krb5-user` for hdfs-krb.               |
| `registry_username` | (required)                           | Caller passes `${{ github.actor }}` — composite actions don't see `github.*`. |
| `registry_password` | (required)                           | Caller passes `${{ secrets.GITHUB_TOKEN }}`.                    |
| `registry`          | `ghcr.io`                            | Override for non-GHCR registries (testing).                    |

## Wraps

- `tests/e2e/common/build-csi.sh` — `rustup target add` + `cargo build --release --package mntrs-csi --target <triple>` + `cp` into docker context
- `docker/login-action@v3` — registry login
- `docker/build-push-action@v6` — build + push

## Why no `github.*` or `secrets.*` defaults?

Composite actions have access to `inputs.*` and `secrets.*` but **not**
`github.*` in their `action.yml` template expressions. Workflow context
(`github.actor`, `github.repository_owner`, etc.) is only visible in the
callsite. So the caller interpolates those and passes them as
`registry_username` / `registry_password`.

## Example

```yaml
- uses: ./.github/actions/setup-csi-image
  with:
    target: x86_64-unknown-linux-musl
    image_tag: ghcr.io/${{ github.repository_owner }}/mntrs-csi:ci-test-${{ github.sha }}
    registry_username: ${{ github.actor }}
    registry_password: ${{ secrets.GITHUB_TOKEN }}
```
