# Release Notes: `restate curl` and `restate http` call your services with the current profile

## New Feature

### What Changed
Two new commands run curl or httpie against the ingress of the current profile, with its URL and
credential filled in:

```shell
restate curl /restate/call/Greeter/greet --json '"Alice"'
restate http POST /restate/call/Greeter/greet name=Alice
restate -p prod curl /restate/call/Counter/my-key/get
```

Pass the ingress path first. Everything after it goes to curl or httpie unchanged, and the output,
exit code and Ctrl-C handling are the tool's own. `restate` options such as `-p/--profile` go before
the path. With `restate http`, the method may come before the path, as with httpie.

### Why This Matters
Calling a handler meant looking up the ingress URL and a token for the right environment and
pasting both into a curl command. The CLI already knows them for every profile, including the
Restate Cloud environments imported by `restate cloud login`.

### Impact on Users
- curl (for `restate curl`) or httpie (for `restate http`) must be installed and on `PATH`.
- When the profile has a credential, it is passed to the tool on its command line, so other users
  of the same machine can see it in the process list while the request runs.
- Restate Cloud: the login token of `restate cloud login` can invoke services when you are an
  administrator of the account.
