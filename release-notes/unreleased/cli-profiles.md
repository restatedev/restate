# Release Notes: `restate profile` manages the servers the CLI talks to

## Breaking Change

### What Changed
The `restate` CLI talks to a *profile*: a section of its config file naming a server. A profile is
either a Restate Cloud environment, recorded with the user and account that reach it, or a
self-hosted server. One command group manages them:

```shell
restate profile list              # the profiles, with user, account and environment for Cloud ones
restate profile describe [name]   # a profile's URLs (the current one when no name is given)
restate profile use [name]        # switch; asks which one when no name is given (local included)
restate profile add staging --admin-url https://restate.example.com:9070 --token "$TOKEN"   # a self-hosted server
restate profile remove <name>
restate profile rename <old> <new>
```

Cloud profiles come from `restate cloud login` and only from there. The browser decides who you log
in as; back in the terminal the CLI imports every environment that user can reach, one profile each,
named `<email>/<account>/<environment>`, and selects one (the current profile when it is that user's,
the only one, or the one you pick). Logging in again as the same user refreshes the token and the
profiles: new environments appear, ones you can no longer reach disappear, renamed profiles keep their
name. Logging in as another user adds their profiles next to the existing ones, each user with their
own token. `--no-select` imports without changing the current profile.

How the CLI decides which profile a command uses, first match wins:

1. `-p/--profile <name>`
2. `RESTATE_ADMIN_URL`, `RESTATE_INGRESS_URL`, `RESTATE_AUTH_TOKEN` or `RESTATE_HOST`, when any is set
3. `RESTATE_PROFILE`
4. the profile selected with `restate profile use` (or `profile add`, `cloud login`)
5. `local`, a server on this machine

`restate whoami` shows which one applied, and for a Cloud profile the user, account and environment.
`restate config view` prints tokens as `<redacted>`.

### Why This Matters
Which server the CLI talked to was decided in too many places: two half front doors
(`restate config list-environments`/`use-environment` and `restate cloud env configure`), hand-edited
sections for self-hosted servers, a `.env` file anywhere up the directory tree, and one login token
shared by every Cloud environment, so working across accounts or users meant logging in and out. Now
the config file is the only place profiles live, a login brings everything that user can reach, and
nothing in the current directory changes the target.

### Impact on Users
- **"Environment" means a Restate Cloud environment; the CLI's entries are profiles.**
  `-e/--environment` and `RESTATE_ENVIRONMENT` keep working as `-p/--profile` and `RESTATE_PROFILE`
  for now, with a deprecation line on stderr.
- **Run `restate cloud login` once after upgrading.** Cloud profiles written by older versions
  have no user, so Cloud commands on them fail with "written by an older CLI" until you log in.
  The login stores your token under your user, imports your environments as profiles, and adopts
  those older profiles (keeping their name) when they belong to the user who logs in.
- **`.env` files are no longer loaded.** The CLI used to load a `.env` from the current directory or
  its parents. Export the variables instead, or add the server with `restate profile add`. When a
  `./.env` with `RESTATE_*` variables is found, one line on stderr says it isn't loaded. This also
  applies to `restatectl`, which only read `RUST_LOG` and color settings from it.
- **The `RESTATE_*` variables no longer override single settings of the selected profile.** When any
  of `RESTATE_ADMIN_URL`, `RESTATE_INGRESS_URL`, `RESTATE_AUTH_TOKEN` or `RESTATE_HOST` is set, they
  describe the whole server: a missing URL defaults to localhost, and the config file isn't
  consulted. In particular `RESTATE_AUTH_TOKEN` alone now targets `localhost`, not the selected Cloud
  environment with that token; set `RESTATE_ADMIN_URL` as well, as CI setups already do.
- **`restate cloud env configure` no longer does anything**: it prints a line asking you to run
  `restate cloud login` again, which imports all your Cloud environments as profiles. `restate
  profile add` doesn't add Cloud environments either: log in instead. `restate config
  list-environments` and `restate config use-environment` keep working but are hidden and print a
  deprecation line pointing at `restate profile list` / `restate profile use`.
- **The current profile moves into the config file**, as `profile` under `[global]`. The old
  `environment` file next to it is still read when the key is absent and deleted on the next write.
- `restate whoami --json` no longer has the `environment_file` and `loaded_dotenv` fields; its
  `environment` object is now `profile`, and the `cloud` object has `user`, `account` and
  `environment` (`{id, name}`) instead of bare ids. `restate profile list --json` has the same three
  columns.
