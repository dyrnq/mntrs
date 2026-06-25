# csi-failure-dump

On job failure, capture k8s pod logs, `kubectl describe`, events, and
host-side reachability probes, and upload them as a GHA artifact.

Replaces the 3 inline "Dump on failure" steps in csi-e2e.yml.

## Inputs

| Input           | Default            | Notes                                                    |
|-----------------|--------------------|----------------------------------------------------------|
| `kubeconfig`    | `/tmp/kubeconfig`  | kubeconfig path (csi-e2e always uses this).              |
| `namespace`     | `csi-mntrs`        | k8s namespace.                                           |
| `test_pod`      | (required)         | Pod name — `mntrs-csi-e2e`, `mntrs-csi-e2e-hdfs`, etc.   |
| `out_dir`       | `/tmp/csi-dump`    | Local dir to write dump files to.                        |
| `artifact_name` | `csi-failure-dump` | Override if you run multiple failures per job (e.g. matrix). |

## Wraps

- `tests/e2e/common/csi-dump-failure.sh` — captures k8s + docker + host state, writes to stdout AND `$out_dir`
- `actions/upload-artifact@v4` — uploads `$out_dir` as a tarball

## Example

```yaml
- if: always()
  uses: ./.github/actions/csi-failure-dump
  with:
    test_pod: mntrs-csi-e2e
```
