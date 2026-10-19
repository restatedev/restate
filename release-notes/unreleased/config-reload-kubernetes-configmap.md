# Release Notes: Configuration file changes in Kubernetes ConfigMaps are applied

## Bug Fix

### What Changed
`restate-server` reloads its configuration file when the file is a symlink whose target is
replaced, which is how Kubernetes updates a ConfigMap mounted into a pod. Before, the first such
update was noticed but failed to load, and later updates went unnoticed until a restart.

### Impact on Users
Runtime-adjustable settings (for example `rocksdb-total-memory-size`) can now be changed by
updating the ConfigMap that holds `config.toml`, without restarting the pods. Settings that are
only read at startup still need a restart.
