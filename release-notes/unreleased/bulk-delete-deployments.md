# Release Notes: Remove several deployments at once

## New Feature

### What Changed
`restate deployments remove` now accepts several deployment ids and removes them together:

```shell
restate deployments remove dp_14LsPzBd7Uuvbx8wA3wtKoD dp_11pXxBETJoA3YQ1jW8QnSuF
```

The command shows every deployment it is about to remove, asks for confirmation once, and
refuses (unless `--force`) if any of them is not drained yet.

The Admin API gains a matching `POST /deployments/bulk-delete` endpoint, taking
`{"deployment_ids": [...], "force": true}` with up to 1000 deployment ids. It responds with the
deployments that were deleted (`succeeded`) and the ones that were not (`failed`, each with a
`reason` such as `not_found` and an error message). Like `DELETE /deployments/{deployment}`, it
currently only supports `force: true`.

### Why This Matters
Cleaning up old deployments, for example all drained ones, previously required one command or
API call per deployment.

### Impact on Users
- No changes are needed for existing usage: removing a single deployment works as before.
- When used against an older server without the bulk endpoint, the CLI removes the
  deployments one by one instead.
