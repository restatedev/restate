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

Server operators enable the feature under the invoker's service-client options:

```toml
[worker.invoker.gcp-federation]
aws-role-arn = "arn:aws:iam::<account>:role/<federation-role>"
aws-role-session-name = "<a value allowed by the role's trust policy>"
```

Restate validates and captures `gcp-federation` once during node startup, including when it is
absent. Invalid federation configuration fails startup. Changing `aws-role-arn` or
`aws-role-session-name` requires a server restart. Removing `gcp-federation` while registered
deployments depend on it strands those deployments after restart; restore the configuration and
restart the server to recover them.

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

In Google Cloud, create a workload identity pool and AWS provider. Map the AWS ARN to
`google.subject`, and restrict the provider to the expected AWS account and federation role:

```sh
gcloud iam workload-identity-pools create RESTATE_POOL \
  --project=GCP_PROJECT --location=global

gcloud iam workload-identity-pools providers create-aws RESTATE_PROVIDER \
  --project=GCP_PROJECT --location=global \
  --workload-identity-pool=RESTATE_POOL \
  --account-id=AWS_ACCOUNT \
  --attribute-mapping='google.subject=assertion.arn,attribute.aws_role=assertion.arn.extract("assumed-role/{role}/")' \
  --attribute-condition="assertion.account == 'AWS_ACCOUNT' && attribute.aws_role == 'FEDERATION_ROLE'"
```

Grant the exact assumed-role session permission to mint an ID token as the deployment's service
account. Then grant that service account permission to invoke the private Cloud Run service:

```sh
gcloud iam service-accounts add-iam-policy-binding \
  INVOKER_SA@GCP_PROJECT.iam.gserviceaccount.com \
  --role=roles/iam.serviceAccountOpenIdTokenCreator \
  --member="principal://iam.googleapis.com/projects/GCP_PROJECT_NUMBER/locations/global/workloadIdentityPools/RESTATE_POOL/subject/arn:aws:sts::AWS_ACCOUNT:assumed-role/FEDERATION_ROLE/ROLE_SESSION_NAME"

gcloud run services add-iam-policy-binding CLOUD_RUN_SERVICE \
  --project=GCP_PROJECT --region=GCP_REGION \
  --role=roles/run.invoker \
  --member="serviceAccount:INVOKER_SA@GCP_PROJECT.iam.gserviceaccount.com"
```

Finally, register the private deployment with its provider and service account:

```sh
restate dp register https://SERVICE_URL \
  --gcp-workload-identity-provider="//iam.googleapis.com/projects/GCP_PROJECT_NUMBER/locations/global/workloadIdentityPools/RESTATE_POOL/providers/RESTATE_PROVIDER" \
  --gcp-impersonate-service-account=INVOKER_SA@GCP_PROJECT.iam.gserviceaccount.com
```

`--gcp-workload-identity-provider` requires `--gcp-impersonate-service-account`. The deployment URI
is the default ID-token audience; use `--gcp-audience` only when the service requires another value.
A deployment that requests federation on a server without `gcp-federation` configured fails closed
with an actionable error and never sends an unauthenticated fallback request.
