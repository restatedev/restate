# Release Notes for restatedev/restate-cloud#1188: AWS -> GCP workload identity federation

## New Feature

### What Changed

HTTP deployments can now authenticate to a private Cloud Run service from an AWS-hosted Restate
server, without any Google credentials of Restate's own. A deployment sets
`workload_identity_provider` (alongside `impersonate_service_account`) on its Google ID-token
`auth` block, naming the customer's GCP workload identity federation provider. Restate mints the ID
token through the AWS -> GCP trust chain instead of its ambient Application Default Credentials:

1. Assume a shared, operator-configured AWS IAM role (the "broker" role) via the AWS SDK's default
   credential chain.
2. SigV4-sign a `GetCallerIdentity` request as that broker session and wrap it in the AIP-4117
   subject-token envelope Google STS expects, binding the target workload identity provider inside
   `SignedHeaders`.
3. Exchange that envelope at the customer's Google STS workload identity provider for a federated
   access token.
4. Call IAM Credentials `generateIdToken`, impersonating the customer's invocation service account,
   to mint the audience-scoped ID token Cloud Run validates.

The broker role assumption is a single AWS identity shared by the whole process (configured once
in `[gcp-federation]`), not one per deployment: a tenant-controlled deployment picks which GCP
workload identity provider and service account it targets, but never the AWS-side identity used to
reach it. Everything downstream of construction — refresh scheduling, idle eviction, permanent
error handling, the 60-second validity guard — is the same credential registry every other GCP
ID-token path already uses (see restatedev/restate#5151); federation adds a fourth construction
recipe to that registry, not new lifecycle machinery.

### Configuration

Server operators enable the feature with a new optional `[gcp-federation]` config block:

```toml
[gcp-federation]
broker-role-arn = "arn:aws:iam::<account>:role/<broker-role>"
session-name = "<a value the broker role's trust policy can bind tenant isolation to>"
```

A deployment then opts in at registration time:

```
restate dp register https://svc-abc-uc.a.run.app \
  --gcp-impersonate-service-account caller@project.iam.gserviceaccount.com \
  --gcp-workload-identity-provider "//iam.googleapis.com/projects/N/locations/global/workloadIdentityPools/P/providers/R"
```

`--gcp-workload-identity-provider` requires `--gcp-impersonate-service-account`: the
external-account credential the federation chain produces cannot mint an ID token ambiently, so
registration is rejected without it. A deployment that requests federation on a server with no
`[gcp-federation]` configured fails registration and every subsequent mint attempt with an
actionable, permanent error — never an unauthenticated fallback request.

### Impact on Users

- No behavior change for deployments that do not set `workload_identity_provider`: the cache key,
  construction path, and error handling for ambient and impersonated (non-federated) ID tokens are
  unchanged.
- Federated deployments add one shared `sts:AssumeRole` session per process; the resulting broker
  credentials are cached and refreshed independently of any single deployment's ID-token
  credential, so many federated deployments do not multiply `AssumeRole` traffic.

### Related Issues

- restatedev/restate-cloud#1188: AWS -> GCP workload identity federation for private Cloud Run
  invocation.
- Builds on restatedev/restate#5151 (the GCP credential registry).
