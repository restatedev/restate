# Release Notes: Advertised Host Values Are Checked at Startup

## Bug Fix

### What Changed

Restate now checks `advertised-host` values when it loads the configuration. This applies to the
root `advertised-host`, `--advertised-host` and `RESTATE_ADVERTISED_HOST`, the `advertised-host`
settings of the `[admin]` and `[ingress]` sections, and `networking.ctrl-advertised-host`. Each
value must be a host name, an IPv4 address, or an IPv6 address in brackets, such as
`[2001:db8::7]`.

### Why This Matters

Previously, some invalid values crashed the node at startup, for example a value containing a
space, a value with a port such as `host:8443`, or an IPv6 address without brackets. Others let
the node start but advertise a wrong address: an empty value, a value with user information such as
`user@host`, or a value with a path such as `host/rpc`. Other nodes then could not reach it.

### Impact on Users

- Valid configurations are not affected.
- A configuration with an invalid advertised host now fails at startup with an error that quotes
  the value, instead of crashing or advertising a wrong address.
- An empty `advertised-host` or `RESTATE_ADVERTISED_HOST` is now rejected. Remove the setting to
  let Restate infer the host.

### Migration Guidance

If a node fails to start with an error saying a value is not a host name or IP address, change the
setting to a bare host. To advertise a different scheme, port or path, use `advertised-address`
instead.
