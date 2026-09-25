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

Credentials come from the environment, in this order:

- a service account key file named by `GOOGLE_SERVICE_ACCOUNT`, or its JSON contents in
  `GOOGLE_SERVICE_ACCOUNT_KEY`
- an Application Default Credentials file named by `GOOGLE_APPLICATION_CREDENTIALS`, or else the
  file `gcloud auth application-default login` writes
- the instance metadata server, when running on Google Cloud, including GKE Workload Identity

Credentials files must be service account keys or `gcloud` user credentials. Workload identity
federation configuration files (`"type": "external_account"`) are not supported, and Restate
fails to start with them. The `aws-*` object store options apply to `s3://` only, and the
`GOOGLE_*` variables apply to every `gs://` client in the process, including snapshots.

GCS object store clients now also use the configured retry policy, such as
`metadata-client.object-store-retry-policy` or `worker.snapshots.object-store-retry-policy`, where
they previously used the object store library's defaults. With Restate's default policy, retries
are no longer bounded by the library's 3-minute retry timeout.

### Maturity

The GCS backend passes Restate's Jepsen linearizability tests running against a real
bucket, with network partitions, process kills and pauses, and found no violations.
While we are confident in the implementation, be aware that we do not yet have extensive
production experience with this feature.

### Impact on Users

- GCS allows about one write per second to a single object and throttles faster writes with
  HTTP 429. Metadata writes are normally far less frequent, but sustained bursts to one key, for
  example heavy node churn updating `nodes_config`, are throttled on GCS where S3 is not.
- Existing `s3://` deployments are unaffected.

### Related Issues

- Issue #5334: Support GCS for the object-store metadata backend
- PR #5335: Original contribution by @ismymiddlename
