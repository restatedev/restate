# Release Notes for Issue #3927: Helm Chart Default Resource Limits Changed

## Breaking Change

### What Changed

The Restate Helm chart default resource limits have been significantly increased to provide better out-of-the-box performance:

| Resource | Previous Default | New Default |
|----------|------------------|-------------|
| Memory limit | 3Gi | **8Gi** |
| Memory request | 1Gi | **8Gi** |
| CPU limit | 1 | **6** |
| CPU request | 500m | **4** |
| `RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE` | (not set) | **3Gi** |

### Why This Matters

**Scheduling impact**: The increased resource requests (8Gi memory, 4 CPU) mean pods may no longer be schedulable on nodes that previously had sufficient resources. This is especially relevant for:
- Development/test clusters with smaller nodes
- Clusters with limited available resources
- Existing deployments that were running with the previous lower defaults

**Performance improvement**: The new defaults are sized for production workloads and ensure RocksDB has adequate memory (<50% of container limit) for optimal performance.

### Impact on Users

**Existing deployments upgrading the Helm chart**:
- **Pods may fail to schedule** if your nodes don't have 8Gi memory and 4 CPU cores available
- If you were relying on the previous defaults (3Gi memory limit, 1Gi request), your pods will require significantly more resources after the upgrade
- Helm upgrade will attempt to recreate pods with new resource requirements

**New deployments**:
- Will use the new production-ready defaults
- Ensure your cluster has nodes with sufficient resources (at least 8Gi memory, 4+ CPU cores)

**Custom configurations**:
- If you already specified custom `resources` in your values file, your settings will continue to be used
- If you provision Restate with a different container memory limit, update `RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE` to be within ~20-50% of that limit

### Migration Guidance

**Option 1: Accept the new defaults (recommended for production)**

Ensure your cluster has nodes with sufficient resources. The new defaults are sized for production workloads.

**Option 2: Keep previous resource limits**

If you need to maintain the previous resource limits (e.g., for development clusters), explicitly set them in your values file:

```yaml
env:
  - name: RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE
    value: "1Gi"  # ~<50% of 3Gi memory limit

resources:
  limits:
    cpu: 1
    memory: 3Gi
  requests:
    cpu: 500m
    memory: 1Gi
```

**Option 3: Custom sizing**

Scale resources based on your workload. Maintain the 75% ratio for RocksDB memory:

| Container Memory | Recommended RocksDB Memory | CPU (suggested) |
|------------------|---------------------------|-----------------|
| 4Gi | 3Gi | 2 |
| 8Gi (new default) | 6Gi (new default) | 4-6 |
| 16Gi | 12Gi | 8 |
| 32Gi | 24Gi | 16 |

```yaml
env:
  - name: RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE
    value: "<75% of memory limit>"

resources:
  limits:
    cpu: <your-cpu-limit>
    memory: <your-memory-limit>
  requests:
    cpu: <your-cpu-request>
    memory: <your-memory-request>
```

### Related Issues

- [#3927](https://github.com/restatedev/restate/pull/3927): Set default value of RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE in helm chart
- [#3925](https://github.com/restatedev/restate/pull/3925): Warn if cgroup memory limit is misaligned with RocksDB memory limit
