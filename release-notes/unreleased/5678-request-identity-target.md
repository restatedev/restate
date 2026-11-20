# Release Notes for PR #5678: Request identity tokens are bound to the exact request

## Improvement

### What Changed

Restate signs a request identity token on every call to a deployment. The token now also records
the HTTP method and the exact path and query of the request it was issued for. Deployments that
verify the token keep working without changes, because verifiers that do not know the new claim
ignore it.

### Why This Matters

Deployments reached through a Restate Cloud tunnel are protected more strictly. An updated tunnel
client only accepts a token for the exact request it was issued for. A token issued for a different
environment, tunnel, host, port, path, query or method is refused with `401 Unauthorized`.

### Impact on Users

- SDKs and other request identity verifiers need no changes.
- An updated Restate Cloud tunnel client checks the new claim when the server provides it. With an
  older server, it only checks that the request path ends with the endpoint the token was issued
  for, such as `/invoke/Greeter/greet`.
- Behind an updated tunnel client, register the deployment with
  `restate deployments register --tunnel-name <name> <service URL>`. A deployment registered with an
  extra path prefix in front of the tunnel gets `401 Unauthorized`.

### Migration Guidance

No action is needed for most users. If you run a Restate Cloud tunnel client, such as
`restate cloud environments tunnel`, check that each deployment behind it is registered with
`--tunnel-name` and no extra path prefix before you upgrade the tunnel client.

### Related Issues

- PR #5678: Bind request identity to the exact request target
