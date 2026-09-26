# Dependency upgrade follow-ups — September 2026

The September 18 audit compared workspace dependencies and the resolved Cargo
graph against non-yanked stable releases in the crates.io sparse index. Existing
version requirements were retained where a lockfile update was sufficient,
except for the explicitly raised Tokio minimum of 1.53.1.

## Deliberately retained versions

| Dependency | Retained version | Reason / follow-up |
| --- | --- | --- |
| GoogleTest | 0.10 | Newer releases change the matcher trait and reference matching. Migrate the custom matchers and tests as separate work. |
| OpenTelemetry family | 0.31, contrib 0.23, tracing-opentelemetry 0.32 | OpenTelemetry 0.32 removes explicit trace/span IDs from `SpanBuilder`. Design a replacement preserving persisted invocation span contexts before upgrading. |
| Reqwest in tracing-instrumentation | 0.12 | Required by OpenTelemetry 0.31's `HttpClient` implementation. Other direct users use 0.13.5. |
| rust-rocksdb | Restate fork 0.51.1 | Port Restate's additional commits onto upstream rust-rocksdb 0.53 before changing the pin. |
| rdkafka | Restate fork 0.38 | Reconcile the newer fork's SASL/zlib build paths and native librdkafka changes; validate Linux/MUSL builds before changing the pin. |
| protobuf | 2.28 | Coupled to the pinned Raft fork and its generated protobuf types. It cannot be independently replaced with protobuf 3. |
| Arrow | 59.3 | Matches DataFusion 55's Arrow 59 requirement. Arrow 60 would introduce a second major version. |

Two compatible-series transitive updates remain blocked by exact upstream
requirements: `crypto-common 0.1.7` requires `generic-array =0.14.7`, and
`axum 0.8.9` requires `matchit =0.8.4`. Update their parents when these constraints
are relaxed rather than patching the lockfile by hand.

## Object storage split

DataFusion core, execution, and protobuf support unconditionally depend on
`object_store 0.13`. Its execution crate enables filesystem support even with
DataFusion's default features disabled. Restate's custom query tables do not
register cloud object stores with DataFusion.

Restate's snapshot and metadata clients use `object_store 0.14.2` with AWS, Azure,
and GCP support. The retained 0.13 instance resolves with only `fs`, `tokio`, and
`walkdir`; its older cloud/XML dependencies are no longer enabled. The graph now
uses only `quick-xml 0.41`, allowing removal of the RUSTSEC-2026-0194 and
RUSTSEC-2026-0195 exceptions.

## Footprint and audit caveats

- Removed 19 unused dependency declarations, including obsolete workspace-only
  declarations. Removing an unused declaration does not necessarily remove a
  transitive package from the graph.
- Removed embedded benchmark CPU profiling via `pprof`; use the external
  profiling instructions in [the benchmark guide](../../benchmarks/README.md).
  The separate jemalloc heap-profiling integration remains in use.
- The mlua upgrade removes `proc-macro-error2`, allowing its obsolete advisory
  exception to be removed as well.
- The default-feature `cargo metadata` graph decreased from 997 to 982 packages
  (including workspace packages). This is a resolved-package count, not a binary
  size or runtime-memory measurement.
- Text searches alone produce false positives: `regress` is used by Typify's
  generated code, and `clap_complete` is referenced by the `completion_commands!`
  macro. Both are required and retained.
- Older versions required by third-party parents remain where necessary. In
  particular, the audit does not force all transitive dependencies to their
  newest major versions.

## Validation

Each upgrade layer was checked before committing with `cargo check`,
`cargo nextest run --all-features`, and `cargo deny --all-features check` when
dependency files changed. Rust changes also passed `cargo fmt --all -- --check`
and `cargo clippy --all-features --all-targets --workspace -- -D warnings`.
Workspace-hack was regenerated after manifest changes.

Local proxy and chaos tests occasionally hit timing limits. Failed runs were
investigated and rerun; later full-suite runs used `-j 1` to avoid concurrent
test load. Tool-timeout/SIGTERM interruptions are not treated as passing runs.
No tests were newly ignored or disabled.
