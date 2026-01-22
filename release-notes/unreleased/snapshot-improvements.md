# Release Notes: Snapshot Improvements

## New Features

### What Changed

Two new capabilities for partition snapshots:

1. **Time-based snapshot intervals**: Configure automatic snapshots based on elapsed wall-clock time, in addition to the existing record-count trigger.

2. **Azure Blob Storage and Google Cloud Storage support**: Snapshots can now be stored in Azure Blob Storage or GCS, in addition to Amazon S3.

### Why This Matters

- **Flexible scheduling**: Time-based intervals ensure snapshots happen regularly even during low-activity periods
- **Multi-cloud support**: Use native cloud storage without needing S3-compatible proxy services
- **Simplified operations**: Native authentication with Azure AD or GCP service accounts

### Impact on Users

- **New configuration option**: `worker.snapshots.snapshot-interval` for time-based triggers
- **New storage destinations**: `az://` / `azure://` for Azure, `gs://` for GCS
- **Breaking**: The undocumented `file://` protocol for snapshot destinations has been removed

### Configuration

**Time-based snapshot intervals:**
```toml
[worker.snapshots]
destination = "s3://my-bucket/snapshots"

# Time-based interval (new)
snapshot-interval = "1h"

# Record-based interval (existing)
snapshot-interval-num-records = 100000
```

When both options are configured, **both conditions must be satisfied** before a snapshot is created. This prevents excessive snapshots during low-activity periods.

**Azure Blob Storage:**
```toml
[worker.snapshots]
destination = "az://my-container/snapshots"
# or
destination = "azure://my-container/snapshots"
```

Configure authentication via environment variables:
- `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`
- Or use Azure AD / managed identity

**Google Cloud Storage:**
```toml
[worker.snapshots]
destination = "gs://my-bucket/snapshots"
```

Configure authentication via environment variables:
- `GOOGLE_SERVICE_ACCOUNT` (path to service account JSON)
- Or use Application Default Credentials (ADC)

**S3 (existing):**
```toml
[worker.snapshots]
destination = "s3://my-bucket/snapshots"
```

### Migration Guidance

- **Existing S3 users**: No changes required
- **Azure/GCP users**: Configure the appropriate destination URL and authentication environment variables
- **`file://` users**: The `file://` protocol is no longer supported. Use S3-compatible local services like MinIO for testing.

### Related Issues

- [#3918](https://github.com/restatedev/restate/pull/3918): Add support for time-based snapshot intervals
- [#3906](https://github.com/restatedev/restate/pull/3906): Enable Azure and GCS object-store protocols for partition snapshots
