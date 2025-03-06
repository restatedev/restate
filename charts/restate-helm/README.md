# Restate

Helm chart for Restate as a single-node StatefulSet.

## Installing

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm --namespace restate --create-namespace
```

# Running a replicated cluster
You can find example values for a 3-node replicated cluster in [replicated-values.yaml](./replicated-values.yaml)

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm --namespace restate --create-namespace -f replicated-values.yaml
```
