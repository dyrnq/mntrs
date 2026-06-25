# upload-log-on-failure

Upload a log file (or directory) to GHA artifacts only on job failure.
Replaces the recurring "Upload X log (failure)" pattern used throughout
the CI workflows.

## Inputs

| Input  | Required | Notes                                            |
|--------|----------|--------------------------------------------------|
| `name` | yes      | GHA artifact name.                               |
| `path` | required | Log file or directory path.                      |

## Why not just `uses: actions/upload-artifact@v4` with `if: failure()`?

You can, and you should — that's the underlying mechanism. This action
exists so that the `if: failure()` + `uses: actions/upload-artifact@v4`
+ `with: { name, path, if-no-files-found }` 4-line block becomes a single
line at the callsite, and so that "if the dump directory is empty
because csi-dump-failure didn't run, still upload" is a single-source-of-
truth default.

## Example

```yaml
- if: always()
  uses: ./.github/actions/upload-log-on-failure
  with:
    name: mount-log-hdfs
    path: /tmp/mntrs-mount-hdfs.log
```
