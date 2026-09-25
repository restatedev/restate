# Remove unused runtime trace export

## Deprecation

The inactive runtime trace exporter and its configuration options
`tracing-runtime-endpoint`, `tracing-json-path`, and `tracing-filter` have been
removed. Remove these unused settings from configuration files.

The corresponding command-line flags remain accepted temporarily for compatibility.
They have no effect and emit a deprecation warning stating that they will be
removed in **v1.9.0**. They no longer appear in normal command-line help and are
not forwarded into configuration.

User-invocation tracing continues to use `tracing-services-endpoint`, or
`tracing-endpoint` as its fallback, with the existing sampling and exporter
settings. Invocation parent relationships and explicitly linked invocation spans
are preserved. These links no longer carry the obsolete `restate.runtime`
attribute. No migration is required for user-invocation tracing.
