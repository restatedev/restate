# Release Notes: Query parameters for SQL introspection queries

## New Feature

### What Changed
The Admin API `POST /query` endpoint accepts an optional `params` field with values for
placeholders in the SQL query. Pass a JSON array to fill positional placeholders (`$1`, `$2`, ...)
or a JSON object to fill named placeholders (`$name`). Values can be strings, numbers, booleans
or `null`.

```json
{
  "query": "SELECT id, status FROM sys_invocation WHERE target_service_name = $1 AND status = $2",
  "params": ["Greeter", "suspended"]
}
```

```json
{
  "query": "SELECT * FROM sys_invocation WHERE id = $id",
  "params": { "id": "inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz" }
}
```

`restate sql` exposes positional placeholders with the repeatable `--param` (`-p`) flag. The CLI
sends each value as a string, and the server converts it to the type of the column it is compared
with. Where the query gives no column type, add a cast, e.g. `LIMIT $1::BIGINT`.

```shell
restate sql "SELECT id, status FROM sys_invocation WHERE target_service_name = \$1 AND status = \$2" \
  -p Greeter -p suspended
```

### Why This Matters
Tools and scripts that build queries from user input no longer have to escape values and
splice them into the SQL string.

### Impact on Users
- Existing requests without `params` behave as before.
- `--param` requires a server that supports query parameters. Older servers ignore the values and
  fail the query because its placeholders have no values.
