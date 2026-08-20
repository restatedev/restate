# Release Notes for restatedev/restate-cloud#1188: AWS -> GCP workload identity federation

## New Feature

### What Changed

HTTP deployments can now authenticate to a private Cloud Run service from an AWS-hosted Restate
server, without any Google credentials of Restate's own. A deployment sets
`workload_identity_provider` (alongside `impersonate_service_account`) on its Google ID-token
`auth` block, naming the customer's GCP workload identity federation provider. Restate mints the ID
token by assuming a shared, operator-configured AWS IAM role (the "broker" role), SigV4-signing a
`GetCallerIdentity` request as that session, exchanging it at the customer's Google STS workload
identity provider, and impersonating the customer's invocation service account via IAM Credentials
`generateIdToken` -- instead of using its ambient Application Default Credentials. The broker role
assumption is shared by the whole process, not one per deployment; construction, refresh, idle
eviction, and error handling reuse the same credential registry every other GCP ID-token path
already uses (restatedev/restate#5151).

### Configuration

Server operators enable the feature under the invoker's service-client options:

```toml
[worker.invoker.service-client.gcp-federation]
broker-role-arn = "arn:aws:iam::<account>:role/<broker-role>"
session-name = "<a value the broker role's trust policy can bind tenant isolation to>"
```

A deployment then opts in at registration time:

```
restate dp register https://svc-abc-uc.a.run.app \
  --gcp-impersonate-service-account caller@project.iam.gserviceaccount.com \
  --gcp-workload-identity-provider "//iam.googleapis.com/projects/N/locations/global/workloadIdentityPools/P/providers/R"
```

`--gcp-workload-identity-provider` requires `--gcp-impersonate-service-account`. A deployment that
requests federation on a server with no `gcp-federation` configured fails registration and every
subsequent mint attempt with an actionable error -- never an unauthenticated fallback request.

### Related Issues

- restatedev/restate-cloud#1188: AWS -> GCP workload identity federation for private Cloud Run
  invocation.
- Builds on restatedev/restate#5151 (the GCP credential registry).
