# CLI: Fix `--remote-port` in `restate cloud env tunnel`

## Bug Fix

### What Changed

`--remote-port` was completely broken — it rejected all input and displayed `[possible values: Self::DEFAULT_PORT, Self::DEFAULT_PORT]`. It now correctly accepts `8080` (ingress) and `9070` (admin).

A new `--no-remote-ports` flag has been added to disable environment port proxying while keeping only the inbound tunnel direction.
