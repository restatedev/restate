# Release Notes for Issue #4755: Native Google Cloud Run authentication for HTTP deployments

## New Feature

### What Changed

HTTP deployments registered with Restate may now opt into native Google
OIDC ID-token authentication. When configured, Restate mints a
Google-signed ID token for every discovery and invocation request to
the deployment and attaches it as
`X-Serverless-Authorization: Bearer <token>`. Cloud Run validates this
header in precedence over `Authorization` and strips it before
forwarding the request to the container, so any `Authorization` value
the operator placed in the deployment's `additional_headers` passes
through to the workload unchanged.

Two configuration knobs are added to each HTTP deployment:

- `impersonate_service_account`: an SA email Restate impersonates via
  the IAM Credentials `generateIdToken` API. Defaults to the ambient
  ADC identity.
- `audience`: an explicit OIDC `aud` claim. Defaults to the deployment
  URL's origin (scheme + host + optional port).

The CLI gains three new flags on `restate dp register`:

```bash
# Cloud Run service callable by the ambient identity:
restate dp register https://svc-abc-uc.a.run.app --gcp-id-token

# Impersonate a target service account:
restate dp register https://svc-abc-uc.a.run.app \
  --gcp-impersonate-service-account caller@my-proj.iam.gserviceaccount.com

# Custom domain in front of Cloud Run; override the audience:
restate dp register https://api.acme.com/svc \
  --gcp-audience https://svc-abc-uc.a.run.app
```

Credentials are discovered through Google's Application Default
Credentials (ADC) chain, but not every ADC source can mint an OIDC ID
token on its own. The ambient `--gcp-id-token` path (no
`--gcp-impersonate-service-account`) requires an ADC source whose
underlying credentials can mint ID tokens directly: service-account
JSON keys, and the GCE / GKE / Cloud Run metadata server, are the
supported ambient sources. External-account (Workload Identity
Federation) and authorized-user (gcloud user creds) sources cannot
mint ID tokens for arbitrary audiences on their own; pair them with
`--gcp-impersonate-service-account` so Restate calls the IAM
Credentials `generateIdToken` API on a target service account that
the ambient identity is authorized to impersonate.

To reconfigure `auth` on an existing deployment (rotate the
impersonation target, change the audience, or remove auth entirely),
re-register with `--force`. This matches how the existing Lambda
`assume_role_arn` flow works:

```bash
# Change the impersonation target:
restate dp register https://svc-abc-uc.a.run.app --force \
  --gcp-impersonate-service-account new-caller@p.iam.gserviceaccount.com

# Remove auth:
restate dp register https://svc-abc-uc.a.run.app --force
```

ID tokens are cached per `(impersonate_service_account, audience)` on
the invoker (Unbounded), and not cached on the admin/discovery path
(matches the existing Lambda `AssumeRoleCacheMode` pattern). Cached
tokens are evicted 60 seconds before expiry.

### Why This Matters

Until now the only way to invoke a private Cloud Run service was to
embed a long-lived bearer token in the deployment's
`additional_headers`. That pattern has no rotation, no audit story,
and forces users to ship service-account keys to the Restate node.
Native ID-token auth replaces it with short-lived, audience-scoped
Google-signed tokens that the receiving service can verify against
Google's JWKS.

### Impact on Users

- Existing deployments are unaffected. The `auth` field is opt-in and
  defaults to absent; HTTP deployments that do not set it continue to
  send requests with no extra headers.
- Lambda deployments are unaffected; the GCP flags are HTTP-only and
  the REST handler and CLI both reject the combination.
- Operators upgrading to this release see no new mandatory
  configuration. Restate will not contact any Google API or read any
  ADC source until the first deployment with `auth` set is observed.

### Migration Guidance

For Restate Cloud and GKE workload-identity-bound deployments, no
additional configuration is needed beyond enabling `auth` on the
deployment.

For self-hosted Restate calling private Cloud Run services, ensure:

- The Restate process can reach Google's metadata server, OR
- `GOOGLE_APPLICATION_CREDENTIALS` points to a service-account JSON
  key file readable by the Restate user.

The principal whose identity ends up in the minted token must hold:

- `roles/run.invoker` on the target Cloud Run service (required for
  Cloud Run IAM to accept the request).
- `roles/iam.serviceAccountOpenIdTokenCreator` on the impersonation
  target, if `impersonate_service_account` is used.

For local development, run `gcloud auth application-default login`
to populate ADC. This is distinct from `gcloud auth login`, which
only populates user credentials for `gcloud` itself and does NOT
populate ADC. Note that authorized-user ADC credentials cannot mint
ID tokens with arbitrary audiences without impersonation; set
`--gcp-impersonate-service-account` for the dev flow.

Cloud Run services exposed via custom domains, custom ports, or
traffic tags should set `--gcp-audience` explicitly to the canonical
service URL. The default-derived audience is the deployment URL
origin, which does not match the canonical Cloud Run identity in
those cases.

### Related Issues

- Issue #4755: Support authenticated invocation of private GCP Cloud
  Run services
- Issue #4448: Original tracking of multi-cloud auth surface
