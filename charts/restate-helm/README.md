# Restate

Helm chart for Restate as a single-node StatefulSet.

## Installing

```bash
helm install restate oci://ghcr.io/restatedev/restate-helm --namespace restate --create-namespace
```

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
