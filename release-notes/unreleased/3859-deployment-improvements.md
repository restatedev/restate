# Release Notes: Deployment Registration and Management Improvements

## New Feature

### What Changed

This release introduces significant improvements to deployment registration and management, providing better safety, flexibility, and observability.

#### Idempotent Deployment Registration

Re-registering the same deployment without changes now returns HTTP 200 instead of an error, making deployment scripts and CI/CD pipelines more robust.

#### New `breaking` Flag

A new `breaking` flag has been added to deployment registration. Use this when you need to allow breaking changes (e.g., changing a service type) while still protecting against accidental overwrites.

#### Default for `force` Changed to `false`

The `force` parameter for **deployment registration** now defaults to `false`. This provides better protection against accidentally overwriting deployments in production.

#### Deployment Metadata

You can now attach arbitrary metadata to deployments during registration for improved observability. When deploying from GitHub Actions, Restate automatically captures environment information (repository, commit, workflow) to link deployments back to their source.

#### Update Deployment API

A new `PATCH /deployments/{id}` endpoint allows updating deployment settings without re-registering:
- **Update headers**: Fix expired tokens or rotate credentials without re-deploying
- **Update assume_role_arn**: Change AWS IAM role configuration for Lambda deployments
- **Update endpoint address**: Redirect invocations to a different URL

### Why This Matters

- **Safer deployments**: Default `force=false` prevents accidental overwrites
- **Better CI/CD integration**: Idempotent registration simplifies deployment scripts
- **Improved observability**: Deployment metadata links back to source control
- **Operational flexibility**: Update credentials and configuration without service disruption

### Impact on Users

- **API version**: These changes are part of Admin API V3. The CLI automatically uses V3.
- **Existing scripts**: Scripts using `force=true` explicitly will continue to work unchanged
- **New scripts**: Scripts relying on the previous `force=true` default should be updated to explicitly pass `--force` if needed

### Usage

For detailed usage examples and deployment workflows, see the [deployment flows documentation](https://docs.restate.dev/services/deployment/overview).

**CLI - Register with breaking changes allowed:**
```bash
restate deployments register http://localhost:9080 --breaking
```

**CLI - Force overwrite:**
```bash
restate deployments register http://localhost:9080 --force
```

**Update deployment headers:**
```bash
curl -X PATCH "http://localhost:9070/deployments/{id}" \
  -H "Content-Type: application/json" \
  -d '{"additional_headers": {"Authorization": "Bearer new-token"}}'
```

### Related Issues

- [#3859](https://github.com/restatedev/restate/pull/3859): Deployments improvements
- [#3144](https://github.com/restatedev/restate/issues/3144): Service deployment / registration issues
