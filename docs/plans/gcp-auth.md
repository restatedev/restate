# GCP Authentication: Implementation Plan

Implements requirements in `docs/requirements/gcp-auth.md`. Strategy:
vertical slice first, then breadth.

## Reference points

- AWS Lambda parity surface:
  - `crates/types/src/config/aws.rs` (cluster config)
  - `crates/types/src/deployment.rs:36-48` (per-deployment fields)
  - `crates/types/src/schema/deployment.rs:150-155` (persisted type)
  - `crates/service-client/src/lambda.rs:44-298` (token client + cache)
  - `crates/service-client/src/lib.rs:156` (endpoint dispatch)
  - `crates/admin-rest-model/src/deployments.rs:89-570` (REST shapes)
  - `crates/admin/src/rest_api/deployments.rs:57-173` (REST handler)
  - `crates/types/src/schema/registry/mod.rs:133-392` (registry merge)
  - `crates/types/src/schema/metadata/updater/mod.rs:476-480` (codec)
  - `crates/service-protocol-v4/src/discovery.rs:217` (discovery)
  - `crates/invoker-impl/src/lib.rs:318-319` (invoker wiring)
  - `crates/node/src/roles/admin.rs:102-107` (admin role wiring)

- New dependency: `google-cloud-auth = { version = "1.10",
  features = ["idtoken"] }`. Pulls in reqwest+rustls (aws-lc-rs).

## Phase A: Vertical slice (priority 1)

Goal: a single integration test that:

1. Stands up an in-process HTTP server impersonating the GCE metadata
   server's `instance/service-accounts/default/identity?audience=...`
   endpoint, returning a self-signed JWT.
2. Stands up a second in-process HTTP server impersonating a
   Cloud-Run-style upstream deployment, asserting it receives an
   `Authorization: Bearer <jwt>` header with the expected audience.
3. Constructs a Restate `ServiceClient` that, given a deployment with
   `auth = Some(HttpAuth::GoogleIdToken { ... })`, calls the upstream
   with the bearer attached.

### A1. Add dependency

- `Cargo.toml` workspace dep: `google-cloud-auth = { version = "1.10",
  features = ["idtoken"] }`.
- `crates/service-client/Cargo.toml`: enable `google-cloud-auth`.
- Run `cargo check` to confirm it builds (do not panic if it pulls a
  large dependency tree; that's expected).
- `cargo hakari generate` if workspace-hack needs updating (T1, defer).

### A2. Token-mint client

New file `crates/service-client/src/gcp.rs`:

```rust
pub enum IdTokenCacheMode {
    None,
    Unbounded,
}

pub struct GcpTokenClient {
    cache_mode: IdTokenCacheMode,
    cache: Option<ArcSwap<HashMap<CacheKey, CachedToken>>>,
    quota_project: Option<String>,
}

impl GcpTokenClient {
    pub fn from_options(opts: &GcpOptions, mode: IdTokenCacheMode) -> Self { ... }

    pub async fn mint(
        &self,
        impersonate_sa: Option<&str>,
        audience: &str,
    ) -> Result<String, GcpAuthError> { ... }
}
```

Internally constructs `google_cloud_auth::credentials::idtoken::Builder::new(audience).build()`
on demand, or `idtoken::impersonated::Builder::from_source_credentials(...)` when SA is set.
Cache by `(Option<String>, String)`. Eviction by token expiry minus 60s skew.

### A3. HttpAuth schema variant

In `crates/types/src/deployment.rs` (and the schema variant in
`crates/types/src/schema/deployment.rs`):

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HttpAuth {
    GoogleIdToken(GoogleIdTokenAuth),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GoogleIdTokenAuth {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub impersonate_service_account: Option<ByteString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<ByteString>,
}
```

`DeploymentType::Http` gains:
```rust
#[serde(default, skip_serializing_if = "Option::is_none")]
auth: Option<HttpAuth>,
```

### A4. Endpoint dispatch

`crates/service-client/src/lib.rs`: extend `Endpoint::Http` with an
optional `HttpAuth` (Phase A: just `Option<HttpAuth>`; full
shape later). The HTTP path (`crates/service-client/src/http.rs`)
calls `gcp_client.mint(...)` and sets the `Authorization` header
(or `X-Serverless-Authorization` if conflict) before sending.

Audience derivation lives in a helper free function in `gcp.rs`.

### A5. ServiceClient wiring

`crates/service-client/src/lib.rs` constructs a `GcpTokenClient`
sibling to `LambdaClient`. Default cache mode = `Unbounded` (slice
defaults to worker behavior; admin/discovery wiring is Phase B).

### A6. Integration test

`crates/service-client/tests/gcp_id_token_attach.rs`:

```rust
#[tokio::test]
async fn id_token_attached_to_invocation() {
    let metadata_server = mock_metadata_server();  // returns a JWT signed with a test key
    let cloud_run_upstream = mock_cloud_run_upstream();  // captures requests

    std::env::set_var("GCE_METADATA_HOST", metadata_server.host());

    let client = build_service_client_with_auth();
    client.invoke(deployment_with_auth(cloud_run_upstream.uri())).await.unwrap();

    let req = cloud_run_upstream.recorded_request();
    assert!(req.headers.contains_key("authorization"));
    let bearer = req.headers["authorization"].to_str().unwrap();
    assert!(bearer.starts_with("Bearer "));
    let claims = decode_jwt_payload(&bearer[7..]);
    assert_eq!(claims["aud"], cloud_run_upstream.origin());
}
```

Mock metadata server is a tiny `axum` or `hyper` server returning:
```json
{"access_token":"...","expires_in":3600,"token_type":"Bearer"}
```
for the `/computeMetadata/v1/instance/service-accounts/default/token`
path (we may need this) and a self-signed JWT for the `/identity`
path. We can self-sign using the `jsonwebtoken` crate which is already
in the tree.

### A7. Commit checkpoint

Commit message: "vertical slice: GCP ID token minted from metadata
emulator reaches HTTP deployment". CI not required to be green at this
point; just the new test plus `cargo check` for the whole workspace.

## Codex plan-review notes (incorporated)

- Auth insertion stays in `ServiceClient::call`; `http.rs` is left as
  a clean transport boundary. (Already applied in slice.)
- Slice does NOT require `GcpOptions` config or `dp update` CLI.
  (Already deferred.)
- Phase B/E must include a rustls provider smoke test asserting that
  the aws-lc-rs default provider satisfies both the existing
  `HttpClient` and the new `google-cloud-auth` reqwest path. The
  google-cloud-auth crate uses `default-rustls-provider = aws-lc-rs`
  by default, which aligns with restate's choice; the smoke test
  protects against future drift.
- Validation work (REQ-VAL-01..07) lands BEFORE the new `dp update`
  CLI in Phase B; the CLI's merge-fetch behavior depends on
  effective-state validation being in place.

## Phase B: REST + schema registry + CLI (priority 2)

Once the slice runs, broaden surface:

### B1. REST model
- Extend `RegisterDeploymentRequest::Http` and `UpdateDeploymentRequest::Http`
  with `auth: Option<Option<HttpAuth>>` (tri-state via
  `serde_with::rust::double_option`).
- Extend `DeploymentResponse::Http` and `DetailedDeploymentResponse::Http`
  with `auth: Option<HttpAuth>`.

### B2. REST handler
- `crates/admin/src/rest_api/deployments.rs`: extract `auth` from
  payload, validate, plumb into registry call.

### B3. Schema registry merge
- `crates/types/src/schema/registry/mod.rs`: update merge logic to
  branch on three states (absent/null/value) for `auth`.
- `UpdateDeploymentAddress::Http` carries `auth: Option<Option<HttpAuth>>`.

### B4. Schema updater
- `crates/types/src/schema/metadata/updater/mod.rs`: map
  `DeploymentAddress::Http` to `DeploymentType::Http` with `auth`.

### B5. CLI
- `cli/src/commands/deployments/register.rs`: add
  `--id-token`, `--impersonate-service-account`, `--audience` flags.
- New `cli/src/commands/deployments/update.rs`: full `dp update`
  subcommand with same flags plus `--no-id-token`, `--uri`.
- `cli/src/ui/deployments.rs`: render `auth` rows in describe.

### B6. Validation rules
- New `crates/admin/src/rest_api/deployments/gcp_auth_validation.rs`
  module with the 7 validation rules (REQ-VAL-01..07).
- Wired into both register and update handlers.

### B7. Cache mode per call site
- `crates/node/src/roles/admin.rs`: build `GcpTokenClient` with
  `IdTokenCacheMode::None`.
- `crates/invoker-impl/src/lib.rs`: build with `Unbounded`.
- Confirm secondary invoker site exists and wire same.

### B8. Discovery dispatch
- `crates/service-protocol-v4/src/discovery.rs`: ensure `auth` is
  carried through to the dispatcher (mirrors the `assume_role_arn`
  hand-off at line 217).

### B9. GcpOptions config
- `crates/types/src/config/gcp.rs`: minimal struct with
  `quota_project: Option<String>`. Flatten or nest into
  `CommonOptions` to match `AwsLambdaOptions` placement.

## Phase C: Docs (priority 3)

### C1. Release notes
- `release-notes/unreleased/4755-gcp-id-token-auth.md` in project
  style. Include both IAM roles, ADC explanation, audience override
  caveat, quota_project caveat.

### C2. Docs
- README sweep if needed for cli-util CLI style guide adherence.

## Phase D: Tests beyond the slice (priority 3)

REQ-TEST-04..09:
- Validation rule unit tests
- Storage backfill round-trip serde test
- CLI flag conflict tests (Lambda + GCP flags)
- Audience derivation algorithm tests
- Discovery auth attach test
- No-fallback-on-mint-failure test

## Phase E: Quality gates (priority 4)

- `cargo check` (per crate, then workspace)
- `cargo nextest run --all-features`
- `cargo fmt --all -- --check`
- `cargo clippy --all-features --all-targets --workspace -- -D warnings`
- `cargo deny --all-features check`
- `cargo hakari generate` if Cargo.toml changed

## Decisions to defer

Tracked in `docs/plans/gcp-auth-decisions.md`. Don't block on:
- TLS provider conflict resolution (D1)
- Hyper-vs-reqwest interop (D2)
- `quota_project` plumbing into the SDK (T11)
- External account / WIF support (T12)
- All-three-invoker-call-sites wiring (T3)

These get addressed during their respective phases. If any of them
trip the e2e slice, route around with a TODO.
