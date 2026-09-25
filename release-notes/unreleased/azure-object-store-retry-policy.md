# Release Notes: Azure Blob Storage snapshots use the configured retry policy

## Behavioral Change

### What Changed

Object store clients for `az://` destinations now use the configured retry policy, such as
`worker.snapshots.object-store-retry-policy`, as `s3://` and `gs://` clients already do.
Previously they used the object store library's defaults and ignored the configured policy.

### Impact on Users

- Deployments with an `az://` snapshot destination and the default retry policy now retry up to
  10 times with exponential backoff from 100ms capped at 10s, with no overall time limit. The
  library defaults were 10 retries with backoff capped at 15s, stopping after 3 minutes.
- A custom `object-store-retry-policy` now takes effect for `az://` destinations.
- `s3://` and `gs://` destinations are unaffected.
