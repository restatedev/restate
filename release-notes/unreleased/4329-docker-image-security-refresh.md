# Release Notes for Issue #4329: Weekly Docker Image Security Refresh

## Improvements > Stability and Security

### What Changed

Restate Docker images are now automatically refreshed weekly to include the latest security updates from the base operating system. This applies to all maintained release versions.

Additionally, all Docker images now include a date-suffixed tag (e.g., `1.6.0-20260204`) to identify when the image was built.

### Why This Matters

Container base images (like `debian:trixie-slim`) receive regular security patches. Previously, users could only get these updates by waiting for a new Restate release. Now, security fixes are automatically incorporated into existing release versions through weekly rebuilds.

### Image Tags

Each Restate release now produces the following Docker tags:

| Tag | Example | Description |
|-----|---------|-------------|
| Version | `1.6.0` | Always points to the latest build of this version |
| Version + Date | `1.6.0-20260204` | Specific build from a particular date |
| Minor | `1.6` | Latest patch release of this minor version |
| `latest` | `latest` | Latest stable release overall |

### Impact on Users

#### To Receive Automatic Security Updates

Configure your container runtime to always pull the latest image:

**Kubernetes:**
```yaml
spec:
  containers:
    - name: restate
      image: docker.io/restatedev/restate:1.6.0
      imagePullPolicy: Always  # Ensures latest security patches
```

**Docker Compose:**
```yaml
services:
  restate:
    image: docker.io/restatedev/restate:1.6.0
    pull_policy: always  # Ensures latest security patches
```

With `Always` pull policy, your deployments will automatically receive security updates when pods restart or services are redeployed.

#### To Pin to a Specific Image

If you prefer deterministic deployments and control when updates are applied, pin to a specific dated tag:

```yaml
# Pin to a specific build date
image: docker.io/restatedev/restate:1.6.0-20260204

# Or pin to an exact image digest
image: docker.io/restatedev/restate:1.6.0@sha256:abc123...
```

With pinned images, you control exactly when security updates are applied by updating the tag in your deployment configuration.

### Refresh Schedule

- **Frequency**: Weekly (every Monday at 00:00 UTC)
- **Scope**: All maintained release branches
- **Skip conditions**: Refresh is skipped if the base image hasn't changed

### Related Issues

- Issue #4329: Update Restate container images regularly to include latest security fixes
