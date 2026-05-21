# Release Notes: Expose applied rule-book and schema versions on `partition_state`

## New Feature

### What Changed
`partition_state` now exposes two additional columns:

- `applied_rule_book_version` — the rule-book version currently applied by the
  partition processor (set once the first `UpsertRuleBook` command has been
  observed).
- `applied_schema_version` — the schema version currently applied by the
  partition processor.

The same two columns are also surfaced as `RULE-BOOK` and `SCHEMA` in
`restatectl partition list`. Internally, the values flow through new
`last_applied_rule_book_version` and `last_applied_schema_version` fields on
`PartitionProcessorStatus`.
