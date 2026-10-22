# Separate control address advertisement

## New Feature

### What Changed

Restate nodes now publish a control address in cluster metadata when provisioning
or joining. By default, it is the same address and port as the fabric advertised
address, so no configuration change is needed.

### Configuration

To advertise a different route for control RPCs, such as a node-specific HTTPS
proxy, configure:

```toml
[networking]
ctrl-advertised-address = "https://node-7.example.com"
```

Alternatively, set `networking.ctrl-advertised-host` to change only the hostname while
using the shared listener's actual port and TLS mode. A full control advertised
address takes precedence over the hostname override.

The same overrides are available as `--ctrl-advertised-host` and
`--ctrl-advertised-address`, or through `RESTATE_NETWORKING__CTRL_ADVERTISED_HOST` and
`RESTATE_NETWORKING__CTRL_ADVERTISED_ADDRESS`. Restart the node to publish a changed
advertisement. Control RPCs still share the existing fabric listener; these
options do not open another port.

### Impact on Users

This prepares discovery for control-aware clients. Existing clients can continue
using the fabric address; this change does not switch `restatectl` discovery to
the separate control address.
