# Release Notes: Send signals to invocations from the CLI

## New Feature

### What Changed
The new `restate invocations signal` commands resolve or reject a named signal of an invocation:

```shell
# Resolve the signal, with an optional payload (inline, from a file, or from stdin with `-`)
restate invocations signal resolve inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz approved --payload '{"ok": true}'
restate invocations signal resolve inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz approved --payload-file payload.json

# Reject the signal with a failure message, and optionally a code (defaults to 500)
restate invocations signal reject inv_1gdJBtdVEcM942bjcDmb1c1khoaJe11Hbz approved --message 'Request denied' --code 403
```

Like the other invocation commands, they support `--dry-run`, `--yes` and `--json`.

### Why This Matters
You can unblock or fail a handler waiting on a signal without writing code to send it, for example when doing manual approvals or recovering stuck workflows.

### Impact on Users
- The commands need a Restate server at version 1.8.2 or later.
- If the invocation is already completed or doesn't exist, the signal is ignored.
