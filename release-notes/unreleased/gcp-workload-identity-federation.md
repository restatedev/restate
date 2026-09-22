# AWS-to-GCP workload identity federation

## New Feature

### What Changed

AWS-hosted Restate servers can now authenticate HTTP deployments to private Cloud Run services
without storing Google credentials. The authentication chain is:

```text
ambient AWS identity
  -> assume the operator-configured AWS federation role
  -> sign an AWS GetCallerIdentity subject token
  -> exchange it through the deployment's Google workload identity provider
  -> impersonate the deployment's Google service account to mint an ID token
```

The assumed AWS role session is shared across the process. Google access-token sources are shared
per workload identity provider and remain live only while cached ID-token credentials reference
them.

### Configuration

Server operators enable registration with a top-level experimental flag and configure the AWS
federation identity under the invoker's service-client options:

```toml
experimental-enable-gcp-workload-identity-federation = true

[worker.invoker.gcp-federation]
aws-role-arn = "arn:aws:iam::<account>:role/<federation-role>"
aws-role-session-name = "<a value allowed by the role's trust policy>"
```

### Impact on Users

The experimental flag is an admission gate: removing it prevents new federated registrations but
does not disable already-registered deployments. Using federation persists deployment metadata
that Restate v1.7 does not understand, so a server that has registered such a deployment cannot
safely roll back to v1.7.

Restate validates and captures the `[worker.invoker.gcp-federation]` configuration once during
node startup, including when it is absent. Invalid federation configuration fails startup. Changing
`aws-role-arn` or `aws-role-session-name` requires a server restart. Removing
`[worker.invoker.gcp-federation]` while registered deployments depend on it strands those
deployments after restart; restore the configuration and restart the server to recover them.

### Usage

Restate obtains its ambient AWS identity from the default AWS SDK credential chain. That identity
must be allowed to assume the configured federation role, and the federation role's trust policy
must trust the ambient IAM user or role. If `aws sts get-caller-identity` reports an STS
`assumed-role` session, use its underlying IAM role ARN in the trust policy. The federation role
itself needs no permission policy because `sts:GetCallerIdentity` requires none.

Use a stable `aws-role-session-name`. Google maps the resulting assumed-role session ARN as the
federated subject:

```text
arn:aws:sts::AWS_ACCOUNT:assumed-role/FEDERATION_ROLE/ROLE_SESSION_NAME
```

In Google Cloud, configure an AWS workload identity provider that maps this assumed-role session
to `google.subject`. Grant that principal
`roles/iam.serviceAccountOpenIdTokenCreator` on the deployment's service account, and grant the
service account `roles/run.invoker` on the private Cloud Run service.

Finally, register the private deployment with its provider and service account:

```sh
restate dp register https://SERVICE_URL \
  --gcp-workload-identity-provider="//iam.googleapis.com/projects/GCP_PROJECT_NUMBER/locations/global/workloadIdentityPools/RESTATE_POOL/providers/RESTATE_PROVIDER" \
  --gcp-impersonate-service-account=INVOKER_SA@GCP_PROJECT.iam.gserviceaccount.com
```

`--gcp-workload-identity-provider` requires `--gcp-impersonate-service-account`. The deployment URI
is the default ID-token audience; use `--gcp-audience` only when the service requires another value.
The Restate CLI refuses to send federation configuration to a server that does not advertise Admin
API v5 with the `gcp_workload_identity_federation` experimental feature enabled.
A deployment that requests federation on a server without
`[worker.invoker.gcp-federation]` configured fails closed with an actionable error and never sends
an unauthenticated fallback request.
