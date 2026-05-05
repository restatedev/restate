# Release Notes for Issue #4535: Remove schema registry v1 data model

## Breaking Change

The legacy v1 schema registry data model (introduced in v1.4 and superseded
by an in-place migration on any registry write) has been removed. Restate
now refuses to start if it finds a registry still in v1 format.

### Migration Guidance

If startup panics with the v1 schema registry message: roll back to v1.6,
perform any write to the schema registry (e.g. update a deployment header)
to trigger the in-place migration, then upgrade again.
