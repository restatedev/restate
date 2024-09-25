# Local cluster runner

This is a tool that aids in running clusters of Restate nodes on your local machine.

Featureset:
- Random port allocation for ingress/admin/pgsql so as to avoid conflicts
- Node services via unix sockets
- Wiring up all metadata clients to point to the node hosting the metadata service
- Finding binaries in the target directory for use in integration tests/examples/from cargo run
- Obtain loglines as a stream based on a regex

The cluster runner adds some extra files to the base dir:
- `$BASE_DIR/metadata.sock`: a unix socket for the metadata service, if a node with the metadata role
  is present.
- `$BASE_DIR/$NODE_NAME/config.toml`: The configuration for the node, passed via env var `RESTATE_CONFIG`
- `$BASE_DIR/$NODE_NAME/node.sock`: The gRPC node service. This is advertised as a absolute unix path.
- `$BASE_DIR/$NODE_NAME/restate.log`: The stdout and stderr of the server process

## Examples
The local cluster runner can be used as a library, as shown in [`examples/two_nodes_and_metadata.rs`](./examples/two_nodes_and_metadata.rs).
You can run this example with
`cargo run --example two_nodes_and_metadata -- --nocapture`.

You can watch node logs with `tail -f restate-data/*/restate.log`
