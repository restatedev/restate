# Azure Blob Storage for Cluster Metadata

## New Feature

### What Changed
The object-store metadata backend now accepts `az://` and `azure://` paths for Azure Blob Storage.

```toml
[metadata-client]
type = "object-store"
path = "az://my-container/metadata"
```

### Impact on Users
Azure storage authentication is read from the environment. Configure
`AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY`, or use Azure AD / managed identity.
Existing S3 and Google Cloud Storage configurations are unchanged.
