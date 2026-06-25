# setup-k3s-test-env

Install k3s, FUSE prerequisites, and prepare a kubeconfig for csi-e2e and
csi-integration jobs. Wraps the shared scripts under `tests/e2e/common/`
so each callsite is a single `uses:` line.

## Inputs

| Input        | Default              | Notes                                                   |
|--------------|----------------------|---------------------------------------------------------|
| `retries`    | `3`                  | k3s install retries (csi-integration overrides to `1`). |
| `kubeconfig` | `/tmp/kubeconfig`    | Output kubeconfig path. csi-integration uses `~/.kube/config`. |
| `fuse_mode`  | `csi-e2e`            | Pass to `install-linux-deps.sh` (1st arg). Use `standard` for csi-integration. |

## Wraps

- `tests/e2e/common/install-k3s.sh` — k3s install + retry loop + kubeconfig copy
- `tests/e2e/common/install-linux-deps.sh` — FUSE kernel module, userland tools, `/dev/fuse`, `user_allow_other`

## Example

```yaml
- uses: ./.github/actions/setup-k3s-test-env
  with:
    retries: '3'
    kubeconfig: /tmp/kubeconfig
```
