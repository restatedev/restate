# GCP Authentication for HTTP Deployments

Requirements for invoking private Google Cloud Run services (and any HTTP
endpoint that validates Google-signed OIDC ID tokens) from Restate.

Tracks issue restatedev/restate#4755. Format: EARS notation.

This feature mirrors the existing AWS Lambda authentication path
(per-deployment `assume_role_arn`) end-to-end: GCP `auth` is configured
at registration; reconfiguration goes through `dp register --force`.

## Terms

- **ADC**: Google Application Default Credentials. The discovery order
  used by Google libraries: `GOOGLE_APPLICATION_CREDENTIALS` env var,
  gcloud user credentials at
  `~/.config/gcloud/application_default_credentials.json`, GCE/GKE
  metadata server, or external account (workload identity federation)
  sources.
- **ID token**: a Google-signed OIDC JWT whose `aud` claim is bound at
  mint time.
- **Impersonation**: minting an ID token whose subject is a different
  service account than the caller's, via the IAM Credentials
  `generateIdToken` API.
- **Ambient identity**: the principal that ADC resolves to without
  impersonation.

## Scope

In scope:

- A new optional authentication block on HTTP deployments that causes
  Restate to attach a Google-signed OIDC ID token to every request to
  that deployment (discovery and invocation).
- Optional service-account impersonation per deployment.
- Optional audience override per deployment.
- CLI flags on `restate dp register` to set `auth` at registration.

Explicitly out of scope:

- All forms of post-registration auth mutation. Reconfigure via
  `dp register --force`.
- A `[gcp]` cluster configuration block. ADC handles source discovery.
- Token-mint metrics and trace spans.
- Tightening the existing Lambda `assume_role_arn` shape.
- Non-Google OIDC providers (Azure AD, AWS Cognito). The schema must
  not preclude adding sibling providers later.
- IAP-specific audience validation.

## 1. Per-deployment authentication model

REQ-DEP-01. The schema for an HTTP deployment SHALL include an optional
`auth` field whose value, when present, identifies a provider-specific
authentication configuration.

REQ-DEP-02. The wire representation of `HttpAuth` SHALL be serde's
externally-tagged enum encoding. The v1 variant tag SHALL be
`"GoogleIdToken"`. This shape allows further provider variants to be
added without altering the encoding of `GoogleIdToken`.

REQ-DEP-03. The `GoogleIdToken` variant SHALL carry two optional fields:
`impersonate_service_account` (string, a service account email) and
`audience` (string, the OIDC `aud` claim).

REQ-DEP-04. WHEN the `auth` field is present and resolves to
`GoogleIdToken` with both members absent or null, the Restate node SHALL
attach a Google-signed ID token minted from ambient ADC identity, with
audience derived from the deployment URL.

REQ-DEP-05. WHEN `impersonate_service_account` is set, the Restate node
SHALL mint the ID token via the IAM Credentials API `generateIdToken`
method targeting that service account.

REQ-DEP-06. WHEN `audience` is set, the Restate node SHALL use that
value verbatim as the `aud` claim on minted tokens.

REQ-DEP-07. WHEN `audience` is absent, the Restate node SHALL derive
the audience as the deployment URI rendered as
`<lowercase-scheme>://<host>[:<port>]`, with default ports omitted,
IPv6 literals kept bracketed, and userinfo/path/query/fragment
discarded. A URI without a host SHALL be rejected by REQ-VAL-01.

## 2. Bearer attachment

REQ-AUTH-01. WHEN sending an HTTP request to a deployment whose `auth`
resolves to `GoogleIdToken`, the Restate node SHALL attach the minted
ID token as `Authorization: Bearer <token>` by default.

REQ-AUTH-02. WHEN the deployment's `additional_headers` already
contains a header named `Authorization` (case-insensitive) AND does
NOT contain a header named `X-Serverless-Authorization`
(case-insensitive), the Restate node SHALL forward the deployment-
provided `Authorization` value unchanged AND SHALL attach the minted
token as `X-Serverless-Authorization: Bearer <token>`.

REQ-AUTH-03. WHEN attaching the token, the Restate node SHALL apply
the bearer to both the service-protocol discovery request and to every
invocation request.

REQ-AUTH-04. IF the Restate node fails to mint a token, THEN the
Restate node SHALL fail the request with an error that names the
deployment and the audience, and SHALL NOT fall back to an
unauthenticated request.

## 3. REST API

REQ-REST-01. The deployment-registration request schema for HTTP
deployments SHALL accept an optional `auth` field of the same shape as
the persisted schema variant.

REQ-REST-02. The deployment-update REST endpoint SHALL preserve the
existing persisted `auth` field across updates that touch other HTTP
deployment fields (uri, additional_headers, use_http_11). The
deployment-update payload SHALL NOT carry an `auth` field in v1;
reconfiguration goes through `dp register --force`.

REQ-REST-03. The deployment-response schema SHALL echo the persisted
`auth` field verbatim when set.

## 4. CLI

REQ-CLI-01. The `restate dp register` command SHALL accept the flags
`--id-token` (boolean), `--impersonate-service-account <SA>`, and
`--audience <AUDIENCE>`. Any of the three SHALL imply `--id-token`.
WHEN supplied AND the endpoint is an HTTP URI, the command SHALL set
the deployment's `auth` field accordingly. WHEN the endpoint is a
Lambda ARN, the CLI SHALL reject the invocation with a non-zero exit
before issuing the REST call.

## 5. Validation at registration

REQ-VAL-01. IF a registration sets `auth` AND the effective URI scheme
is not `https` AND the effective host is NOT the literal hostname
`localhost` and NOT an IP literal in any of 127.0.0.0/8, ::1/128,
10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, fc00::/7, fe80::/10, THEN
the admin REST handler SHALL reject the request with a hard error.

REQ-VAL-02. IF a registration sets `auth` AND `additional_headers`
contains BOTH `Authorization` AND `X-Serverless-Authorization`
(case-insensitive), THEN the admin REST handler SHALL reject the
request with a hard error.

## 6. Schema persistence

REQ-PERSIST-01. The persisted HTTP deployment schema SHALL gain an
`auth` field that is absent in records persisted before this change
AND that deserialises as "no auth configured" when absent.

## 7. Design notes (non-normative)

- The token-mint client is implemented using the official
  `google-cloud-auth` crate with the `idtoken` feature. Swap to a
  custom implementation is acceptable if the crate proves unsuitable.
- The provider-typed schema (`HttpAuth::GoogleIdToken`) exists so that
  adding non-Google ID-token providers later is an additive change.
  Google-specific field names (`audience`, `impersonate_*`) should not
  be promoted to a generic wrapper that hides the provider.
- The `audience` field accepts any non-empty string; Cloud Run supports
  custom audiences that are not URLs and the IAM `generateIdToken`
  API accepts any string. Operators submit clean values (no leading
  or trailing whitespace); typos surface as Cloud Run 401s at first
  invocation.
- Cache: ID tokens are cached per `(impersonate_service_account,
  audience)` on the invoker (Unbounded), and not cached on the admin
  /discovery path (None mode). Eviction is 60s before token expiry;
  the constant lives in `crates/service-client/src/gcp.rs`.
- Per-attempt mint timeout is 10s, hardcoded; retry policy is owned
  by the calling layer (discovery dispatch or invoker).
- Log policy: never log token contents; do not log audience or
  impersonate-SA above DEBUG.
- IAM roles operators need: `roles/iam.serviceAccountOpenIdTokenCreator`
  on the impersonation target (when impersonation is used);
  `roles/run.invoker` on the Cloud Run service being invoked, held by
  the identity that ends up in the minted token.
- Local development uses `gcloud auth application-default login` to
  populate ADC, distinct from `gcloud auth login`. Authorized user
  credentials cannot mint ID tokens for arbitrary audiences without
  impersonation.
- Audience defaults to URL origin. Custom domains, custom ports, and
  traffic tags require an explicit `--audience` override.
