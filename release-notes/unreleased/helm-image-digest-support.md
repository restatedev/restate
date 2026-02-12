# Release Notes: Helm chart image configuration improvements

## Deprecation

### What Changed

The `version` field in the Helm chart values is now deprecated in favor of `image.tag`. A new `image.digest` field has been added to support pinning images by digest (SHA).

### Why This Matters

- **`image.tag`**: More conventional Helm naming, aligns with common chart patterns
- **`image.digest`**: Enables pinning to specific image digests for reproducible deployments and security compliance

### Migration Guidance

**Old approach (deprecated):**
```yaml
version: "1.6.0"
```

**New approach:**
```yaml
image:
  tag: "1.6.0"
```

**Using a digest:**
```yaml
image:
  digest: "sha256:abc123..."
```

The `version` field continues to work for backward compatibility but will be removed in a future release. If both `image.tag` and `version` are set, `image.tag` takes precedence. If `image.digest` is set, it takes precedence over both `image.tag` and `version`. The `image.digest` value must start with `sha256:`.

### Impact on Users

- **Existing deployments using `version`**: Continue to work, but users should migrate to `image.tag`
- **New deployments**: Should use `image.tag` or `image.digest`
- **No breaking changes**: This is a backward-compatible change with a deprecation notice

### Example Usage

```bash
# Use specific tag
helm install restate ./charts/restate-helm --set image.tag=1.6.0

# Use date-suffixed tag (from docker-refresh workflow)
helm install restate ./charts/restate-helm --set image.tag=1.6.0-20260205

# Pin to specific digest
helm install restate ./charts/restate-helm \
  --set image.digest=sha256:abc123def456...

# Use different registry with digest
helm install restate ./charts/restate-helm \
  --set image.repository=ghcr.io/restatedev/restate \
  --set image.digest=sha256:abc123def456...
```
