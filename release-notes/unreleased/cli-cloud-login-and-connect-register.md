# Release Notes: Cloud login, `connect://` registration, `restate cloud connect-sdk`, `restate cloud tunnel`

## New Feature

### What Changed
`restate cloud login` now also selects the Restate Cloud environment the CLI talks to, right after
logging in (see the `restate profile` release note for how environments become profiles).

A service connected to Restate Cloud with the SDK's `connect()` can be registered by its name, with the
identifier the SDK prints at startup:

```shell
restate cloud login
restate deployments register connect://greeter-v1
```

The CLI resolves the name from the Cloud environment you're logged in to; nothing else to configure.

`restate cloud connect-sdk` mints the two keys an SDK needs for the current Cloud environment,
`RESTATE_CONNECT_KEY` (serving with `connect()`) and `RESTATE_INGRESS_KEY` (invoking from code), and
writes them to `restate.env` in the current directory, with each key's id in a comment for when you
want to revoke it. Load the file with your runtime (`node --env-file=restate.env`, `uv run --env-file
restate.env`, your IDE's env file setting) or export it in the shell with `set -a; source restate.env;
set +a`. The file is only created, never changed: when it exists, nothing is minted. `--output` picks
another path; `--stdout` prints the two lines instead of writing a file, to pipe them. The keys never
appear in the command's normal output.

The port-forwarding tunnel for services that can't connect by themselves is now `restate cloud tunnel`;
the tunnel's name is `--name`, and defaults to `<directory>-<hostname>` instead of a
random word, so a restarted tunnel keeps its registered deployment.

### Why This Matters
Going from a fresh laptop to a service registered in your Cloud environment is two commands: log in,
register. You no longer need to configure the environment in between, nor to assemble the tunnel URL
by hand.

### Impact on Users
- `restate cloud env tunnel` and `--tunnel-name` keep working, hidden, with a deprecation line; use
  `restate cloud tunnel --name`. `restate deployments register --tunnel-name <destination URL>` is
  unchanged.
- `restate deployments register` now waits up to 30 seconds for the server to discover the endpoint
  (`--request-timeout` still applies when set higher), so a slow or unreachable endpoint reports the
  server's discovery error instead of a CLI timeout.
- `restate cloud login` fails immediately when it can't ask for input (no terminal, `CI` set,
  `--yes` or `--json`), instead of waiting for a browser login that can't happen. Log in from a
  terminal.
