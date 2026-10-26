# Release Notes: Startup Validation of Advertised Addresses

## Behavioral Change

### What Changed

Restate now checks the advertised addresses in your configuration when a node starts. A node
refuses to start if one of them uses a scheme it cannot connect to, such as `ftp://`. This applies
to `advertised-address`, `ctrl-advertised-address`, the admin and ingress advertised
addresses, and `metadata-client.addresses`.

Restate nodes also accept addresses written as `scheme:/path` in cluster metadata, including
schemes they do not know yet. They keep such addresses unchanged and do not connect to them.

### Why This Matters

Previously, a node with an unsupported advertised-address scheme started normally, and other
nodes failed when they tried to connect to it. The error now appears at startup, on the node that
has the wrong configuration.

Accepting unknown address formats in cluster metadata keeps this version compatible with future
Restate versions that advertise new kinds of addresses during a rolling upgrade or rollback.

### Impact on Users

- Valid configurations are not affected.
- A configuration with an unsupported advertised-address scheme now fails at startup with an error
  that names the setting and the address.
- `grpc+unix:/path` is accepted as an alternative spelling of `unix:/path` for
  `metadata-client.addresses` and for `restatectl --address`. It is not accepted for
  `advertised-address` or `ctrl-advertised-address`, because older Restate versions
  cannot read it.

### Migration Guidance

If a node fails to start with an invalid advertised address error, change the named setting to an
`http://`, `https://` or `unix:` address.
