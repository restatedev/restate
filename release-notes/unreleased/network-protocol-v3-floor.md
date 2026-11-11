# Node-to-node protocol compatibility

## Breaking Change

Restate nodes now require network protocol V3 or later. Nodes that only support
V2 can no longer establish node-to-node connections.

The supported upgrade floor is **Restate 1.7**. All nodes must be running at least
Restate 1.7 before upgrading to this release. If you run an older version, first
follow the supported upgrade path to Restate 1.7. Restate 1.7 already supports V3,
so this change does not restrict rolling upgrades from supported versions.
