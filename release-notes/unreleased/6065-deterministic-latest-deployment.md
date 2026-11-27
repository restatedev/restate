# Release Notes for Issue #6065: Latest deployment of a service is the same on every node

## Bug Fix

### What Changed
Updating a deployment with `overwrite: true` so that it starts serving a service which another
deployment already serves now gives that service a new revision, and the updated deployment
becomes the latest deployment of the service. This matches what registering a deployment does.

Previously, both deployments could end up with the same revision of the service. Nodes could then
disagree on which of the two is the latest deployment, for example when routing new invocations or
when resuming an invocation on the latest deployment.

### Impact on Users
- If two of your deployments already hold the same revision of a service, all nodes now pick the
  same one as the latest: the deployment that was registered last.
- No action is needed. To choose a different latest deployment, force-register it again.

### Related Issues
- Issue #6065: Latest deployment of a service is not deterministic when two deployments hold it at the same revision
