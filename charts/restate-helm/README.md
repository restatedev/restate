# Restate

Helm chart for Restate as a single-node StatefulSet.

## Installing

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm --namespace restate --create-namespace
```

# Resources
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

The environment variable `RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE` should be set such that it is roughly 75% of the memory available.
In the default helm values, this variable is set to `6Gi`.
Under load, Restate will eventually use the entire RocksDB memory size offered to it.

# Running a replicated cluster
You can find example values for a 3-node replicated cluster in [replicated-values.yaml](./replicated-values.yaml).
Please ensure you use a version of that file (based on the git tag of the repo) which matches the version of the helm chart you are deploying.

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm --namespace restate --create-namespace -f replicated-values.yaml
```

Note that you need to explicitly provision the Restate cluster via

```bash
kubectl exec -n restate -it restate-0 -- restatectl provision --replication 2 --yes
```
