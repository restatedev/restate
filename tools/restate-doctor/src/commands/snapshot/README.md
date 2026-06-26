# `restate-doctor snapshot`

An offline tool for inspecting the data inside a Restate **partition snapshot** with SQL — without a
running cluster.

Normally the only way to run SQL over partition data is through a live worker's admin `/query`
endpoint (what `restate sql` talks to). When debugging an incident you often have only the snapshot
repository, not a running cluster. This subcommand points at a snapshot repository (S3 or
compatible), downloads the partitions' latest snapshots, opens the partition store + DataFusion
query layer locally, and drops you into an interactive SQL terminal.

## What it does

1. Connects to the snapshot repository and **auto-discovers** the partitions stored under the prefix.
2. **Auto-detects** the cluster name/fingerprint from the repository so snapshot download validation
   passes (override with `--cluster-name` if needed).
3. For each partition, downloads its latest snapshot and imports it into a local RocksDB store under
   `--cache-dir`. The import is **cached**: a subsequent run against the same `--cache-dir` reuses
   the local store and skips the download.
4. Opens the DataFusion query layer over the imported partitions and either starts an interactive
   REPL (default), runs a single `--query` and exits, or serves the admin-compatible SQL query API
   over HTTP with `--listen` so existing tooling (e.g. `restate sql`) can be pointed at it.

The tool is strictly read-only: the query engine rejects DDL/DML, and it only ever imports and reads
partition data — it never writes back to the repository or mutates snapshot contents.

## Usage

```shell
# Interactive REPL over all partitions in the repository
restate-doctor snapshot s3://my-bucket/snapshots --aws-region us-east-1 --cache-dir /tmp/snap-dbg

# Against a MinIO / S3-compatible endpoint
restate-doctor snapshot s3://my-bucket/snapshots \
    --aws-endpoint-url http://localhost:9000 \
    --aws-allow-http \
    --aws-access-key-id minioadmin \
    --aws-secret-access-key minioadmin \
    --cache-dir /tmp/snap-dbg

# One-shot query, then exit
restate-doctor snapshot s3://my-bucket/snapshots --aws-region us-east-1 \
    --query "SELECT id, target_service_name FROM sys_invocation_status LIMIT 10"

# Restrict to a single partition
restate-doctor snapshot s3://my-bucket/snapshots --aws-region us-east-1 --partition-id 0

# Serve the admin-compatible SQL query API (defaults to 127.0.0.1:9078)
restate-doctor snapshot s3://my-bucket/snapshots --aws-region us-east-1 --listen 127.0.0.1:9078
```

The `--listen` server exposes a single `POST /query` endpoint with the same request/response
contract as a live worker's admin `/query` endpoint: a JSON body `{"query": "<sql>"}`, returning an
Arrow IPC stream by default or JSON when the request carries `Accept: application/json`.

```shell
curl -s http://localhost:9078/query \
    -H 'content-type: application/json' \
    -H 'accept: application/json' \
    -d '{"query":"SELECT service_name, service_key FROM state LIMIT 10"}'
```

In the REPL, terminate statements with `;`. Use `\q` (or Ctrl-D) to exit; Ctrl-C clears the current
statement. Query history is persisted under `<cache-dir>/.snapshot-debugger-history`.

```
sql> SELECT table_name FROM information_schema.tables;
sql> SELECT service_name, service_key, key FROM state LIMIT 20;
```

## Options

| Flag | Description |
| --- | --- |
| `<repository>` | Snapshot repository base URL, e.g. `s3://my-bucket/snapshots` (positional, required). |
| `--cache-dir <dir>` | Local directory where snapshots are imported and cached. Default: `./snapshot-debugger-data`. |
| `--partition-id <id>` | Inspect only this partition instead of all partitions in the repository. |
| `--cluster-name <name>` | Cluster name to validate snapshots against (auto-detected if omitted). |
| `--query <sql>` | Run a single query, print the result, and exit (no REPL). |
| `--listen [<addr>]` | Serve the admin-compatible SQL query API (`POST /query`) instead of the REPL. Defaults to `127.0.0.1:9078` when given without a value. Mutually exclusive with `--query`. |
| `--aws-region <region>` | AWS region of the object store (also inferable from the environment). |
| `--aws-endpoint-url <url>` | Object store endpoint override (required for MinIO and other S3-compatible stores). |
| `--aws-profile <profile>` | AWS configuration profile to use. |
| `--aws-access-key-id <id>` | S3 access key id (or MinIO username). |
| `--aws-secret-access-key <key>` | S3 secret access key (or MinIO password). |
| `--aws-session-token <token>` | S3 session token (for short-term STS credentials). |
| `--aws-allow-http` | Allow plain HTTP to the object store endpoint (required for non-HTTPS endpoints). |

Standard AWS environment variables and config files are honoured via the AWS SDK, so the
`--aws-*` flags are only needed to override them.

## Available tables

Only the partition-store-backed tables are exposed (the ones whose data actually lives in a
snapshot):

- `sys_invocation_status`, `sys_invocation_state`, and the `sys_invocation` view
- `sys_keyed_service_status`, `sys_locks`, `state`
- `sys_journal`, `sys_journal_events`
- `sys_inbox`, `sys_promise`
- `sys_vqueue_meta`, `sys_vqueues`

Use `SELECT table_name FROM information_schema.tables` to list them at runtime, and
`DESCRIBE <table>` to see a table's columns.

The cluster-wide tables (`sys_node`, `sys_partition`, `sys_logs`, …) and the schema-registry tables
(`sys_deployment`, `sys_service`, `sys_rules`) are **not** available: they require a live cluster /
schema registry that a standalone snapshot has no access to. The leader-owned columns of
`sys_invocation_state` (invoker retry/failure state) read as `NULL`, since that state is ephemeral
and never captured in a snapshot.

## Limitations

- Reads the **latest** snapshot per partition; it does not select older retained snapshots.
- A snapshot reflects a point in time (its `min_applied_lsn`); it is not the live state of a running
  partition.
