# Local cluster runner CLI
This tool creates Restate clusters locally from a set of configuration that maps to the `Cluster` struct in
[`crates/local-cluster-runner`](../../crates/local-cluster-runner/README.md). This configuration can be specified
in TOML, but you may also provide a `pkl` file and use the Pkl API to generate valid config in a very concise manner.

Execute with `cargo lcr` or `cargo local-cluster-runner`.

## Examples
```bash
cargo lcr --cluster-file tools/local-cluster-runner/examples/two_nodes_and_metadata.pkl
cargo lcr --cluster-file tools/local-cluster-runner/examples/without_domain_sockets.pkl
cargo lcr --cluster-file tools/local-cluster-runner/examples/three_logserver.pkl
```

## Pkl bindings
The goal is to have a Pkl API which allows rapid prototyping of valid groups of configuration to build a cluster.
This may not exactly match the API of the local-cluster-runner libraries methods for generating test nodes.
We rely on a Pkl schema of runtime configuration in `Configuration.pkl`, which can be updated as follows:

```bash
cargo xtask generate-config-schema \
  | jq '[path(.. | select(.allOf? != null and (.allOf | length  == 1)))] as $paths | reduce $paths[] as $path (.; setpath($path+ ["$ref"]; getpath($path + ["allOf"])[0]["$ref"])) | delpaths([$paths[] | . + ["allOf"]])' \
  | pkl eval ./tools/local-cluster-runner/pkl/generate.pkl \
  > ./tools/local-cluster-runner/pkl/Configuration.pkl
```
