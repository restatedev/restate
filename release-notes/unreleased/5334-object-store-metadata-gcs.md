# Release Notes for Issue #5334: Google Cloud Storage for object-store metadata

## New Feature

### What Changed

The object-store metadata backend accepts `gs://` paths in addition to `s3://`, so cluster
metadata can live in a Google Cloud Storage bucket:

```toml
[metadata-client]
type = "object-store"
path = "gs://<bucket>/<prefix>"
```

Credentials come from the environment: Application Default Credentials via
`GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_SERVICE_ACCOUNT`, or the instance metadata server when
running on Google Cloud. The `aws-*` object store options apply to `s3://` only.

GCS object store clients now also use the configured retry policy, such as
`metadata-client.object-store-retry-policy` or `worker.snapshots.object-store-retry-policy`, where
they previously used the object store library's defaults. With Restate's default policy, retries
are no longer bounded by the library's 3-minute retry timeout.

### Impact on Users

- GCS allows about one write per second to a single object and throttles faster writes with
  HTTP 429. Metadata writes are normally far less frequent, but sustained bursts to one key, for
  example heavy node churn updating `nodes_config`, are throttled on GCS where S3 is not.
- Existing `s3://` deployments are unaffected.

### Related Issues

- Issue #5334: Support GCS for the object-store metadata backend
- PR #5335: Original contribution by @ismymiddlename
