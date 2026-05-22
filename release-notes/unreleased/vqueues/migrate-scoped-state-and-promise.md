# Release Notes: Migrate the unscoped state and promise tables into their scoped variants

## Behavioral Change

### What Changed

A new experimental opt-in, `experimental.migrate_scoped_tables`, switches each
local partition store onto the scoped state and promise tables. When the flag
is enabled, the partition processor runs a one-time, per-partition migration on
open that:

- Copies every row of the legacy unscoped state table (`KeyKind::State`) into
  the scoped state table (`KeyKind::ScopedState`) with `scope = None`.
- Copies every row of the legacy unscoped promise table (`KeyKind::Promise`)
  into the scoped promise table (`KeyKind::ScopedPromise`) with `scope = None`.
- Range-deletes the legacy unscoped ranges and bumps the on-disk schema version
  to `ScopedStateAndPromise` in a single atomic write batch.

After the migration runs, all state and promise reads and writes on the
partition use the scoped tables exclusively, including for services with
`scope = None`.

### Impact on Users

- **Default behavior is unchanged.** With
  `experimental.migrate_scoped_tables = false` (the default), partition stores
  keep the existing mixed-mode layout: services with an explicit scope already
  write to the scoped tables, while services without a scope continue to write
  to the legacy unscoped tables. Downgrades remain possible.
- **Opting in** (`experimental.migrate_scoped_tables = true`) triggers the
  migration on the next partition open. Once a partition has reached
  `ScopedStateAndPromise`, it cannot be downgraded to a server version that
  does not recognize the new schema version — the older binary will refuse to
  open the partition.
