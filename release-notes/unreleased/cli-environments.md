# Release Notes: `restate env` manages the environments the CLI talks to

## Breaking Change

### What Changed
One command group manages the servers the `restate` CLI talks to, its *environments*:

```shell
restate env list                                   # the environments of the CLI config file
restate env use [name]                             # switch; asks which one when no name is given
restate env add                                    # add a Restate Cloud environment (after `restate cloud login`)
restate env add --cloud-environment my-acct/prod   # a specific Cloud environment, by name or id
restate env add staging --admin-url https://restate.example.com:9070 --token "$TOKEN"   # a self-hosted server
restate env remove <name>
restate env rename <old> <new>
```

A Cloud environment is always added under its own name; the name you give is for self-hosted
servers only. `restate cloud login` still selects an environment at the end.

How the CLI decides which environment a command uses, first match wins:

1. `-e/--environment <name>`
2. `RESTATE_ADMIN_URL`, `RESTATE_INGRESS_URL`, `RESTATE_AUTH_TOKEN` or `RESTATE_HOST`, when any is set
3. `RESTATE_ENVIRONMENT`
4. the environment selected with `restate env use` (or `env add`, `cloud login`)
5. `local`, a server on this machine

`restate whoami` shows which one applied. `restate config view` now prints tokens as `<redacted>`.

### Why This Matters
Environments had two half front doors (`restate config list-environments`/`use-environment` and
`restate cloud env configure`), self-hosted servers had to be added by editing the config file, and a
`.env` file anywhere up the directory tree could silently change which server a command hit. Now the
config file is the only place environments live, selection is explicit, and nothing in the current
directory changes it.

### Impact on Users
- **`.env` files are no longer loaded.** The CLI used to load a `.env` from the current directory or its
  parents. Export the variables instead, or add the server with `restate env add`. When a `./.env`
  with `RESTATE_*` variables is found, one line on stderr says it isn't loaded. This also applies to
  `restatectl`, which only read `RUST_LOG` and color settings from it.
- **The `RESTATE_*` variables no longer override single settings of the selected environment.** When any
  of `RESTATE_ADMIN_URL`, `RESTATE_INGRESS_URL`, `RESTATE_AUTH_TOKEN` or `RESTATE_HOST` is set, they
  describe the whole environment: a missing URL defaults to localhost, and the config file isn't
  consulted. In particular `RESTATE_AUTH_TOKEN` alone now targets `localhost`, not the selected Cloud
  environment with that token; set `RESTATE_ADMIN_URL` as well, as CI setups already do.
- **`restate cloud env configure` is removed.** Use `restate env add` (optionally with
  `--cloud-environment`). `restate config list-environments` and `restate config use-environment` keep
  working but are hidden and print a deprecation line pointing at `restate env list` / `restate env use`.
- **The current environment moves into the config file**, as `environment` under `[global]`. The old
  `environment` file next to it is still read when the key is absent and deleted on the next write.
- A Cloud environment whose name is already used by a different environment in the config file can't be
  added until that one is renamed or removed; `restate env rename` doesn't apply to Cloud environments.
- `restate whoami --json` no longer has the `environment_file` and `loaded_dotenv` fields.
