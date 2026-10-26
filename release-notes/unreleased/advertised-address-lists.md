# Advertised addresses for callers in specific locations

## New Feature

### What Changed

A node can now advertise additional addresses for callers in specific locations, next to
`advertised-address`, which older Restate versions keep using. An entry is written
`location@address` for the callers whose own location starts with `location`, written like the
node `location` as `region` or `region.zone`, or as a bare `address` for every caller. A caller
takes the first entry it can use at its closest location, then an entry for every caller, and
only then `advertised-address`.

There are two lists. `ctrl-advertised-addresses` is for tools such as `restatectl`
that reach a node from another network. `advertised-addresses` is for nodes that reach each other
through different networks, for example over a private address within a zone; when no control
list is set, it is advertised for control as well, because control RPCs share the fabric listener.

For Docker Compose, give each node its published port under a location no node lives in, such as
`external`. Once tools select among these addresses, a tool that connects through such an address
will use it for every node:

```yaml
services:
  restate-1:
    ports: ["15122:5122"]
    environment:
      RESTATE_ADVERTISED_ADDRESS: http://restate-1:5122
      RESTATE_CTRL_ADVERTISED_ADDRESSES: external@http://localhost:15122
```

For Kubernetes, one pod template serves every replica because `$(VAR)` expands in an environment
value:

```yaml
env:
  - name: POD_NAME
    valueFrom: { fieldRef: { fieldPath: metadata.name } }
  - name: RESTATE_CTRL_ADVERTISED_ADDRESSES
    value: external@https://$(POD_NAME).restate.example.com
```

For nodes spread over zones and regions, list the fabric addresses in order of preference:

```toml
location = "us-east1.az1"
advertised-address = "https://node-7.example.com"
advertised-addresses = [
  "us-east1.az1@http://10.1.7.3:5122",                 # callers in the same zone
  "us-east1@http://node-7.us-east1.internal:5122",     # callers in the same region
]
```

An address may be just a host or `host:port`; it then defaults to `http` and the default port,
so `us-east1.az1@10.1.7.3` is enough for a private zone address. An environment variable takes
the entries comma-separated, as do the `--advertised-addresses` and `--ctrl-advertised-addresses`
flags, which can also be repeated. Nodes publish their lists in
cluster metadata when they join. Restart a node to publish a changed list. A node refuses to start
if a list has more than 8 entries, has two entries with the same scheme for the same location, or
has an entry with a scheme the node does not serve. With TLS mode `require`, every entry must be
an `https` address, as for `advertised-address`.

### Impact on Users

- Nothing changes unless you set one of the new keys.
- Nodes and tools begin to choose among these addresses in an upcoming change. Until then,
  connections keep using `advertised-address` and `ctrl-advertised-address`.
- Until every metadata server runs Restate 1.9 or later, adding or removing a metadata server
  clears the lists of all nodes, and nodes running older Restate versions may drop them when they
  update cluster metadata. A node notices this and publishes its lists again within seconds.
