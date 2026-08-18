# Limit the number of registered deployments

## New Feature

Operators can now limit the number of deployments in the schema registry with
`admin.num-deployments-limit`:

```toml
[admin]
num-deployments-limit = 100
```

The limit is unset by default, preserving the existing unlimited behavior. Once the configured
limit is reached, Restate rejects registrations that would create another deployment. Re-registering
or overwriting an existing deployment remains allowed, as does deleting deployments to get back
under the limit.

Related issue: [#5184](https://github.com/restatedev/restate/issues/5184).
