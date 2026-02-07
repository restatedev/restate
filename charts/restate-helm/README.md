# Restate

Helm chart for Restate as a single-node StatefulSet.

## Installing

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm --namespace restate --create-namespace
```

## Image Configuration

By default, the chart uses the image version matching the chart version. You can override this using `image.tag` or pin to a specific digest using `image.digest`.

### Using a specific tag

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm \
  --namespace restate --create-namespace \
  --set image.tag=1.6.0
```

### Using a date-suffixed tag

Restate publishes weekly refreshed images with security updates (e.g., `1.6.0-20260205`):

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm \
  --namespace restate --create-namespace \
  --set image.tag=1.6.0-20260205
```

### Pinning to a specific digest

For reproducible deployments or security compliance, you can pin to an exact image digest:

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm \
  --namespace restate --create-namespace \
  --set image.digest=sha256:abc123...
```

### Using a different registry

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm \
  --namespace restate --create-namespace \
  --set image.repository=ghcr.io/restatedev/restate \
  --set image.tag=1.6.0
```

## Resources

Restate's performance is strongly influenced by the CPU and memory available. You can vary the resources in your values file.
The defaults are:

```yaml
resources:
  limits:
    cpu: 6
    memory: 8Gi
  requests:
    cpu: 4
    memory: 8Gi
```

The environment variable `RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE` should be set under 50% of the memory available.
In the default helm values, this variable is set to `3Gi`.
Under load, Restate will eventually use the entire RocksDB memory size offered to it.

## Running a replicated cluster
You can find example values for a 3-node replicated cluster in [replicated-values.yaml](./replicated-values.yaml).
Please ensure you use a version of that file (based on the git tag of the repo) which matches the version of the helm chart you are deploying.

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm --namespace restate --create-namespace -f replicated-values.yaml
```

Note that you need to explicitly provision the Restate cluster via

```bash
kubectl exec -n restate -it restate-0 -- restatectl provision --replication 2 --yes
```
