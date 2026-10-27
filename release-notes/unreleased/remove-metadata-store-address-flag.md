# Remove the `--metadata-store-address` flag

## Breaking Change

### What Changed

`restate-server` no longer accepts the `--metadata-store-address` flag. Passing it now fails at
the command line with an unknown-argument error.

### Why This Matters

The flag has done nothing since August 2024. It was the way to point a node at a separate
metadata store server, and it stopped working when that configuration moved into the
`metadata-client` section, with a list of `addresses` instead of a single address. The flag
stayed in the command line and was silently ignored, so a node started with it as its only
metadata seed came up with no seeds at all: a node with the metadata-server role used its own
address and ran, while a node without that role failed at startup with an assertion. Removing the
flag turns that silent misconfiguration into an immediate error.

### Impact on Users

- A `restate-server` command line or startup script that still passes `--metadata-store-address`
  fails to start until the flag is removed. Such setups were not getting any seed from it.
- Nothing changes for deployments that configure `metadata-client.addresses`.

### Migration Guidance

Configure the metadata seeds through the `metadata-client` section:

```toml
[metadata-client]
addresses = ["http://metadata-node:5122"]
```

or through the environment, where the value is a TOML array:

```sh
RESTATE_METADATA_CLIENT__ADDRESSES='["http://metadata-node:5122"]'
```
