# Release Notes: Fix incorrect row ordering for `ORDER BY id` SQL queries

## Bug Fix

### What Changed
The SQL tables `sys_invocation_status`, `sys_invocation_state`, `sys_journal`, `sys_journal_events`, `sys_inbox`, `sys_vqueue_meta`, and `sys_scheduler` no longer declare the `id` column as pre-sorted to the query engine. The underlying storage orders rows by the binary key encoding, which does not match the lexicographic order of the string-encoded `id` column. DataFusion trusted the declared ordering and skipped sorting for queries whose requested order matched it (e.g. `ORDER BY partition_key NULLS FIRST, id NULLS FIRST`), returning rows in storage order instead of the requested order.

`partition_key` remains declared as sorted; its numeric order genuinely matches the storage order.

### Why This Matters
Queries ordering by `id` could silently return misordered rows. Queries that group or deduplicate on `(partition_key, id)` relied on the same false ordering through sort-based aggregation.

### Impact on Users
- `ORDER BY ... id` queries on the affected tables now always return correctly ordered rows.
- Such queries now include an explicit sort step. Common query forms (the default `ASC`/`DESC` orderings, the CLI's invocation listing, and the `sys_invocation` view join) were already planned with a sort and are unaffected.
- No configuration or code changes are required.
