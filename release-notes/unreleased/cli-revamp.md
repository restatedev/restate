# CLI revamp for scripting, CI, and AI agents

## New Feature / Behavioral Change

The `restate` CLI was reworked to be first-class for scripting, CI, and AI agents, while staying
friendly for humans.

### Structured output (`--json`)

A global `--json` flag prints command output as a single JSON document instead of human tables.
Diagnostics/progress stay on stderr, so `restate <cmd> --json` produces clean, parseable JSON on
stdout. Supported by:

- `services list` / `describe` / `status`, `deployments list` / `describe`,
  `invocations list` / `describe` / `journal`, `state get`
- `whoami` (also reports the connected server's version)
- `sql describe` (and `sql "<query>"`, which already supported `--json`)
- `config view` (emits JSON instead of TOML)
- `services config view` (options with their values and descriptions, plus handler overrides)

JSON is consistent across commands: timestamps are RFC 3339 / ISO-8601 regardless of `--time-format`;
service type uses one key (`service_type`) and one vocabulary (`service` / `virtual_object` /
`workflow`); deployment `protocol` is a `[min, max]` array (not a stringified one);
`invocations describe --json` includes `completion` (success/failure); and collection ordering (e.g.
handlers) is deterministic.

`services status` now derives Active Keys (`locked_keys`, with new `scope` and `lock_acquired_at`
fields) from the vqueues lock tables and omits them on servers without vqueues; the summary adds
a `paused` count and never reports completed invocations.

Where a read-only follow-up is useful (e.g. `invocations list` → `invocations describe <id>`,
`invocations describe` → `invocations journal <id>`), the JSON document carries a top-level
`next_steps` array of `{"command": "...", "description": "..."}` objects, with ready-to-run commands
(real ids filled in, `--json` appended). Human output shows the same suggestions as a tip on stderr.

### Machine-readable errors and exit codes

On failure with `--json`, the CLI emits a JSON error object on stdout —
`{"error": {"kind": "...", "message": "..."}}` — and returns a differentiated process exit code so
scripts/agents can branch on the failure class: `2` invalid usage, `3` confirmation required,
`4` not found, `5` network, `6` auth, `7` aborted (prompt declined), `8` server (5xx), `1` other. Messages are tidied (no
`<UNKNOWN>` placeholder; transport URL / HTTP-status detail is kept in human output but dropped from
the JSON `message`). Errors also suggest read-only next steps (e.g. `whoami` on connection
failures, the matching `list` on not found): a tip in human output, `error.next_steps` in JSON. `whoami` now exits non-zero when the admin health probe fails. `state get` exits `4` (not found) when the key has no state or the service is unknown (a deleted service with leftover state still prints it, with a warning).

### Previewing and confirming changes (`--dry-run` / `--yes`)

Commands that change state (`deployments register` / `remove`, `invocations cancel` / `kill` /
`purge` / `pause` / `resume` / `restart-as-new`, `state clear` / `patch`, `services config patch`)
share one confirmation flow, designed so an agent can preview a change, get its user's approval, and
then apply it:

- `--dry-run` shows the planned changes and exits `0` without applying anything. With `--json`, the
  plan is a `changes` array (e.g. every resolved invocation id and the action to take).
- `--json` without `--yes` no longer prompts: it prints the same plan document
  (`"dry_run": true, "applied": false`, a `hint`, and the ready-to-run `apply_command`) and exits
  with the new code `3` (confirmation required). Non-interactive human runs also exit `3` instead
  of `7`.
- `--yes` applies. With `--json`, the result document carries `"applied": true` plus per-item
  `results` for bulk operations; a partial failure keeps that single document on stdout and exits
  non-zero.
- Interactive human runs are unchanged: preview, then prompt.

Errors carrying a Restate error code (e.g. `META0003`) link to its documentation
(`https://docs.restate.dev/references/errors#meta0003`), shown on its own line and as
`error.docs_url` in JSON.

### New global flags

- `--color <auto|always|never>` to control colored output (previously only environment variables).
- Every command's `--help` lists these under a separate "Global options" heading, after the
  command's own options.
- `--non-interactive` to fail fast instead of prompting; also implied by `--json`, when stdin is not
  a terminal, or when `CI` is set.
  Editor-based commands (`state edit`, `services config edit`, `config edit`, …) fail in this
  mode and point to the matching `patch` command instead.
- Watch mode (`-w`) can't be combined with `--json`.

### Discover the SQL introspection schema from the CLI

`restate sql` ships an embedded reference of the introspection tables:

- `sql describe <table>` prints a table's columns (name, type, description); unknown names error with
  the valid list.
- `sql --help` ends with the list of queryable tables.

The reference is generated at build time from the same source as the online SQL docs, so it matches
the server the CLI was built against. Running queries is unchanged.

### Dedicated `invocations journal` command

`restate invocations journal <id>` is a dedicated, scriptable view of an invocation's journal:

- Head + tail preview by default.
- Inclusive index selector: a single entry (`5`), a range (`1..3`), or open-ended (`..10`, `90..`);
  `--all` shows the whole journal.
- `--payload`/`-p` includes entry payloads; byte-array payloads are decoded to JSON (or UTF-8 text)
  for readability instead of raw `[123, 34, …]` arrays. Combine with `--json` to pipe into `jq`.
- Defaults to the lightweight `entry_lite_json` metadata projection, fetching full payloads only with
  `--payload`.

`invocations describe` now shows a short journal preview and points to `journal` for the full view,
plus the invocation's last timeline event (`event` in `--json`).

Invocations retrying on servers with vqueues are now reported as `backing-off` (with retry count,
next retry and last failure) in `invocations list` / `describe` and the dry-run plans, and
`invocations list --status backing-off` matches them. Malformed invocation ids exit `2`, and
`invocations journal <id> <range>` exits `4` when the range has no entries.

### Other command changes

- `invocations list` lists the most recently modified invocations first. Use
  `--order-by modified|created` and `--order desc|asc` to change it (`--oldest-first` still
  works). It also accepts the same query as `cancel` / `pause` / `purge` (an invocation id,
  or a target prefix like `Cart/alice`), combined with the other filters.
- `deployments describe` / `remove` accept the endpoint URL or Lambda ARN a deployment was
  registered with, as well as its id.
- `example <name>` works non-interactively: without `--output-directory` (alias `--out`) it
  downloads into `./<name>` instead of prompting, and with `--json` prints the example's name,
  directory and README path. An unknown name exits `4` and points to `restate example --list`; an
  existing output directory exits `2` (missing parent directories are created). `--list` can no
  longer be combined with a name or `--output-directory`.
- New `restate openapi` prints the admin API's OpenAPI spec as JSON, so you (or an agent) can
  discover the admin API and call it directly, e.g. with `curl`.

## Why This Matters

Scripts and agents can consume `--json` deterministically, branch on exit codes and parse error
objects (instead of scraping stderr), discover the SQL schema without leaving the terminal, and
inspect journals precisely. Human output stays clean and readable.

## Impact on Users

- Human output was redesigned; scripts should use `--json` rather than parse it.
- `deployments register` now requires Restate server 1.6 or newer; against older servers it
  fails with an error asking to upgrade the server (or to use an older CLI).
- Scripts relying on exit code `7` for a refused prompt in non-interactive mode get `3` now.
- `sql describe` JSON is wrapped in an object (`{"table": {...}, "columns": [...]}`) for
  consistency with the rest of the CLI.
- `invocations describe` shows a journal preview + a hint rather than the full call graph; use
  `invocations journal <id>` for the full journal. The journal view targets the version-2 journal
  format; version-1 journals show only basic entry metadata (type/name, no payloads).
