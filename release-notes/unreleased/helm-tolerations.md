# Helm Chart: Add tolerations support

## New Feature

### What Changed

The Helm chart now supports Kubernetes
[tolerations](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/)
via the `tolerations` values field, allowing Restate pods to be scheduled on
tainted nodes.

### Impact on Users

- No impact on existing deployments (default is an empty list).
- Operators using tainted node pools can now configure tolerations directly
  through Helm values instead of using post-render patches.

### Example

```yaml
tolerations:
  - key: "dedicated"
    operator: "Equal"
    value: "restate"
    effect: "NoSchedule"
```
