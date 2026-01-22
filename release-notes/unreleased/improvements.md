# Release Notes: Improvements

## Improvements

### Observability

**Suspended invocation SQL fields**

New columns in `sys_invocation` and `sys_invocation_status` tables for debugging suspended invocations:
- `suspended_waiting_for_completions`: List of completion IDs the invocation is awaiting
- `suspended_waiting_for_signals`: List of signal indices the invocation is awaiting

```sql
SELECT id, suspended_waiting_for_completions, suspended_waiting_for_signals 
FROM sys_invocation 
WHERE status = 'suspended'
```

- [#4209](https://github.com/restatedev/restate/pull/4209): Add suspended info fields to SQL

**Invoker metrics with partition IDs**

Invoker metrics are now tagged with partition IDs, enabling better attribution and debugging of per-partition behavior.

- [#3883](https://github.com/restatedev/restate/pull/3883): Tag invoker metrics with partition ids

**Kafka consumer lag metrics**

New metrics track Kafka consumer lag for subscriptions, helping monitor ingestion backpressure.

- [#3779](https://github.com/restatedev/restate/pull/3779): Track Kafka consumer lag

### Stability & Security

**Metadata cluster identity validation**

Nodes now validate that they are communicating with members of the same cluster by checking the cluster fingerprint when using the replicated metadata server. 
This prevents accidental cross-cluster communication in shared network environments (e.g., Kubernetes clusters with misconfigured networking).

- [#4189](https://github.com/restatedev/restate/issues/4189): Harden metadata server to not accept incoming messages from other clusters

**Metadata size limits**

Soft (80%) and hard (95%) size limits are now enforced on metadata objects relative to the gRPC message size limit:
- **Soft limit**: Logs a warning with guidance (e.g., "Remove unused deployments to keep schema metadata manageable")
- **Hard limit**: Rejects writes to prevent metadata from becoming too large to synchronize

- [#4093](https://github.com/restatedev/restate/pull/4093): Metadata object size limits

**Memory limit warnings**

On Linux containers, Restate automatically detects cgroup memory limits and warns if `rocksdb-total-memory-size` is misconfigured:
- Warning if < 50% of container memory (underutilized)
- Error if > 90% of container memory (OOM risk)

Recommendation: Set `RESTATE_ROCKSDB_TOTAL_MEMORY_SIZE` to ~75% of container memory.

- [#3925](https://github.com/restatedev/restate/pull/3925): Memory limit warnings

### CLI Improvements

**Service configuration view**

`restate service config view` now displays complete service configuration including:
- Retry policy settings (max attempts, intervals, on-max-attempts behavior)
- Lazy state setting
- Per-handler configuration overrides

- [#3945](https://github.com/restatedev/restate/pull/3945): Service config view improvements

**Deployment `--extra` flag**

`restate deployment describe` and `restate deployment list` now support `--extra` to optionally show deployment status and active invocation counts. By default, these expensive queries are skipped for better performance.

```bash
restate deployment list --extra
```

- [#3875](https://github.com/restatedev/restate/pull/3875): Deployment --extra flag

### Helm Chart

**`podLabels` support**

Added support for custom pod labels in the Helm chart:
```yaml
podLabels:
  app.kubernetes.io/team: platform
```

- [#3812](https://github.com/restatedev/restate/pull/3812): Helm chart podLabels support
