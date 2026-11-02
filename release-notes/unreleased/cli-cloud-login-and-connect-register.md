# Release Notes: Cloud login selects the environment, register connected services by name

## New Feature

### What Changed
`restate cloud login` now also selects the Restate Cloud environment the CLI talks to, right after
logging in. With one account and one environment it does so silently; with several, it asks you to
pick one. If the CLI already uses a Cloud environment, logging in again keeps it: silently when it's
the only environment, otherwise the picker offers to keep it next to the other environments. Pass
`--no-env` to only log in, as before.

A service connected to Restate Cloud with the SDK's `connect()` can be registered by its name, with the
identifier the SDK prints at startup:

```shell
restate cloud login
restate deployments register connect://greeter-v1
```

The CLI resolves the name from the Cloud environment you're logged in to; nothing else to configure.

### Why This Matters
Going from a fresh laptop to a service registered in your Cloud environment is two commands: log in,
register. You no longer need `restate cloud env configure` in between, nor to assemble the tunnel URL
by hand.

### Impact on Users
- Existing environments in your CLI config file are kept. Login writes a section named after the
  selected Cloud environment, replacing one that is that same environment; if the name is already
  used by a different environment, nothing is written and you're pointed to
  `restate cloud env configure`.
- `restate cloud env configure` still works to select another environment or account.
- `--tunnel-name <destination URL>` for services behind `restate cloud env tunnel` is unchanged.
- `restate deployments register` now waits up to 30 seconds for the server to discover the endpoint
  (`--request-timeout` still applies when set higher), so a slow or unreachable endpoint reports the
  server's discovery error instead of a CLI timeout.
- `restate cloud login` fails immediately when it can't ask for input (no terminal, `CI` set,
  `--yes` or `--json`), instead of waiting for a browser login that can't happen. Log in from a
  terminal.
