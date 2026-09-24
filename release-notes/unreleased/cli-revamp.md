# CLI revamp for scripting, CI, and AI agents

## New Feature / Behavioral Change

The `restate` CLI was reworked to be first-class for scripting, CI, and AI agents, while staying
friendly for humans. All output now flows through a single formatter, so every data command supports
both human tables and machine-readable JSON from one code path.

### Structured output (`--json`)

A global `--json` flag prints command output as a single JSON document instead of human tables.
Diagnostics/progress stay on stderr, so `restate <cmd> --json` produces clean, parseable JSON on
stdout. Supported by the data commands, including:

- `services list` / `describe` / `status`, `deployments list` / `describe`,
  `invocations list` / `describe` / `journal`, `subscriptions list` / `describe`,
  `kafka-clusters list` / `describe`, `vqueues list` / `describe`, `rules list`, `state get`
- `whoami` (also reports the connected server's version)
- `sql tables` / `describe` (and `sql "<query>"`, which already supported `--json`)
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
`purge` / `pause` / `resume` / `restart-as-new`, `state clear` / `patch`, `services config patch`,
`kafka-clusters create` / `delete` / `patch`, `subscriptions create` / `delete`, `rules delete`)
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

### New global flags

- `--color <auto|always|never>` to control colored output (previously only environment variables).
- `--non-interactive` to fail fast instead of prompting; also implied by `--json`, when stdin is not
  a terminal, or when `CI` is set.

### Discover the SQL introspection schema from the CLI

`restate sql` ships an embedded reference of the introspection tables:

- `sql tables` lists every queryable table with its column count.
- `sql describe <table>` prints a table's columns (name, type, description); unknown names error with
  the valid list.
- `sql --help` ends with a condensed table list and pointers to those subcommands.

The reference is generated at build time from the same source as the online SQL docs, so it matches
the server the CLI was built against. Running queries is unchanged.

### Dedicated `invocations journal` command

`restate invocations journal <id>` is a dedicated, scriptable view of an invocation's journal:

- Head + tail preview by default (eliding the middle with a `· · · (N more)` marker).
- Inclusive index selector: a single entry (`5`), a range (`1..3`), or open-ended (`..10`, `90..`);
  `--all` shows the whole journal.
- `--payload`/`-p` includes entry payloads; byte-array payloads are decoded to JSON (or UTF-8 text)
  for readability instead of raw `[123, 34, …]` arrays. Combine with `--json` to pipe into `jq`.
- Defaults to the lightweight `entry_lite_json` metadata projection, fetching full payloads only with
  `--payload`.

`invocations describe` now shows a short journal preview and points to `journal` for the full view.
Its last timeline event is rendered for humans (error, code, failing command, a truncated stacktrace,
what a suspended invocation waits on); `--json` carries it as `event` (previously an `events` array).

Invocations retrying on servers with vqueues are now reported as `backing-off` (with retry count,
next retry and last failure) in `invocations list` / `describe` and the dry-run plans, and
`invocations list --status backing-off` matches them. Malformed invocation ids exit `2`, and
`invocations journal <id> <range>` exits `4` when the range has no entries.

### Human output

- `invocations list` (and the recent invocations in `services status <service>`) shows
  each invocation as `[id] target  status` (`succeeded` / `failed` for completed ones,
  retry count when backing off), followed by its created / modified times (and scheduled
  start) and its idempotency key. `--json` still returns the full invocation objects.
- The other list commands (`services`, `deployments`, `subscriptions`, `kafka-clusters`,
  `vqueues`, `rules`, `sql tables`) use the same layout: a `[id] descriptor` column, a few
  short columns, and the secondary details on the lines below. In their `--json` output,
  `subscriptions` `options` is now the options object (it was a count), and `rules` always
  includes `description`, `disabled`, `version` and `last_modified`.
- `invocations list` now lists the most recently modified invocations first. Use
  `--order-by modified|created` and `--order desc|asc` to change it (`--oldest-first` still
  works). It also accepts the same query as `cancel` / `pause` / `purge` (an invocation id,
  or a target prefix like `Cart/alice`), combined with the other filters.
- Service type, visibility and journal states are plain text instead of emojis (`services list`
  gains a `VISIBILITY` column, and drops the `DEPLOYMENT TYPE` column).
- Section-title and status emojis are only shown in colored human output: never with `--json`,
  `--color never`, `NO_COLOR`, or when piped (a text fallback such as `[OK]:` is used instead).
- The journal view (`invocations journal` and `invocations describe`) lists entries as
  ENTRY (`[3]: Call command`), NAME (run / sleep / call names) and WHEN (the first
  entry's age, then each entry's offset from it, e.g. `+12ms`, `+1h`), with details on the
  lines below: state / promise keys, call and send targets, sleep deadlines, failures, and
  for notifications the command they complete.

## Why This Matters

Scripts and agents can consume `--json` deterministically, branch on exit codes and parse error
objects (instead of scraping stderr), discover the SQL schema without leaving the terminal, and
inspect journals precisely. Human output stays clean and readable.

## Impact on Users

- Default human output is unchanged apart from the plain-text service type / visibility labels; `--json` opts into the machine
  format. Scripts that grepped the old emoji / `[public]` / `[private]` glyphs should switch to the
  plain-text labels.
- Scripts relying on exit code `7` for a refused prompt in non-interactive mode get `3` now.
- `kafka-clusters create` / `subscriptions create` accept flags after their `KEY=VALUE` arguments.
- `sql tables` / `describe` JSON is wrapped in an object (`{"tables": [...]}`,
  `{"table": {...}, "columns": [...]}`) for consistency with the rest of the CLI.
- `invocations describe` shows a journal preview + a hint rather than the full call graph; use
  `invocations journal <id>` for the full journal. The journal view targets the version-2 journal
  format; version-1 journals show only basic entry metadata (type/name, no payloads).
