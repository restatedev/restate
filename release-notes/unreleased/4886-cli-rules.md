# Release Notes for Issue #4886: CLI commands for concurrency-limit rules

## New Feature

### What Changed
The `restate` CLI can now manage the action concurrency-limit rulebook
(#4655) directly, instead of hand-crafting REST calls and SQL queries.

```
restate rules | rule    list | set | enable | disable | delete
```

Listing reads the `sys_rules` introspection table; `set`/`enable`/`disable` go
through `PUT /limits/rules` and `delete` through `POST /limits/rules/bulk-delete`.
`set` is an idempotent create-or-update that merges into the existing rule. `set`
(on an existing rule), `enable`, `disable` and `delete` read the rule's current
version first and pass it as an optimistic-concurrency precondition, so concurrent
edits are rejected with an actionable error rather than silently overwritten.

### Examples

```bash
# Create or update a rule (idempotent; merges into the existing rule)
restate rules set "scope1" --concurrency 10 --description "demo"

# Inspect
restate rules list
restate rules list --extra        # adds description, version, last modified

# Modify (preserves the fields you don't touch)
restate rules set "scope1" --concurrency 5
restate rules set "scope1" --unlimited
restate rules disable "scope1"
restate rules enable "scope1"

# Remove
restate rules delete "scope1"
```

Run `restate rules --help` for everything else.

### Related Issues
- Issue #4886: Add support for listing and updating rules in the Restate CLI
- Issue #4655: Rulebook support in the runtime
