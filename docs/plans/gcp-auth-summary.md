# GCP Cloud Run Authentication: Status Summary

Branch: `worktree-gcp-auth` at `/Users/pavel/restate/restate/.claude/worktrees/gcp-auth`.
Issue: restatedev/restate#4755.

## What's done

### User-visible

- `restate dp register <https-uri>` accepts three new flags:
  `--gcp-id-token`, `--gcp-impersonate-service-account <SA>`,
  `--gcp-audience <AUDIENCE>`. Any positive flag implies
  `--gcp-id-token`. Lambda ARN endpoints are rejected up front.
  Negotiated admin API below V5 is rejected up front so an old
  server cannot silently drop the field.
- `restate dp describe <id>` renders Authentication / Impersonation /
  Audience rows when an HTTP deployment carries auth.
- REST `POST /deployments` accepts an `auth` field on HTTP variants
  starting at admin API V5; older versions reject the field.
  Response shapes echo it back.
- Restate mints Google-signed OIDC ID tokens via the official
  `google-cloud-auth` crate and always attaches them as
  `X-Serverless-Authorization: Bearer`. Cloud Run validates that
  header in precedence over `Authorization` and strips it before
  forwarding to the container, so any customer-supplied
  `Authorization` in `additional_headers` passes through to the
  workload unchanged.
- The bearer flows on both discovery and invocation requests.
- Token-mint failures fail the request (no unauthenticated fallback).
- Tokens cached per `(impersonate_sa, audience)` on the invoker;
  not cached on admin/discovery. 60s eviction skew.
- Release notes at `release-notes/unreleased/4755-gcp-id-token-auth.md`.

### Implementation surface

| Crate / file | Change |
|---|---|
| `crates/types/src/deployment.rs` | New `HttpAuth` enum + `GoogleIdTokenAuth` struct; new `derive_audience` helper. |
| `crates/types/src/schema/deployment.rs` | `DeploymentType::Http` gains `auth: Option<HttpAuth>` with serde-default backfill via `serde_hacks`. |
| `crates/types/src/schema/registry/mod.rs` | HTTP update merge preserves persisted `auth` across uri/header/use_http_11 updates (H1 fix). |
| `crates/types/src/schema/metadata/updater/mod.rs` | Plumbs `auth` from `HttpDeploymentAddress` into `DeploymentType::Http`. |
| `crates/service-client/src/gcp.rs` | New `GcpTokenClient` with `IdTokenCacheMode`, ADC + impersonation paths, 10s timeout. |
| `crates/service-client/src/lib.rs` | `Endpoint::Http` carries `Option<HttpAuth>`; `ServiceClient::call` mints and attaches the token as `X-Serverless-Authorization: Bearer` unconditionally. |
| `crates/admin-rest-model/src/deployments.rs` | `auth` on `RegisterDeploymentRequest::Http`, `DeploymentResponse::Http`, `DetailedDeploymentResponse::Http`. |
| `crates/admin/src/rest_api/deployments.rs` | Handler extracts `auth`, runs `validate_http_auth` (REQ-VAL-01/02). |
| `cli/src/commands/deployments/register.rs` | New flags, Lambda preflight rejection. |
| `cli/src/ui/deployments.rs` | `dp describe` rows; delegates audience display to `restate_types::deployment::derive_audience`. |
| `Cargo.toml` (workspace) | New dep `google-cloud-auth = { version = "1.10", features = ["idtoken"] }`. |

### Tests

15 new tests across three locations:

- **Integration** (`crates/service-client/tests/gcp_id_token_attach.rs`): 5 tests covering bearer-attach with derived audience, customer-`Authorization` passthrough alongside the minted X-Serverless-Authorization, explicit audience override, no-op when auth absent, and mint-failure-no-fallback (REQ-AUTH-04).
- **Validation unit tests** (`crates/admin/src/rest_api/deployments.rs`): 5 tests for REQ-VAL-01 (https-or-loopback) and REQ-VAL-02 (dual-Authorization rejection).
- **Storage backfill** (`crates/types/src/schema/deployment.rs`): 2 new tests — pre-auth records deserialise with `auth: None`; auth round-trips intact.
- **Audience derivation** unit tests in `crates/service-client/src/gcp.rs` (pre-existing, 6 tests).

Plus the existing 1296 workspace tests all still pass.

### Quality gates

- `cargo fmt --all -- --check` clean
- `cargo clippy --all-features --all-targets --workspace -- -D warnings` clean
- `cargo nextest run --all-features` 1296 passed / 10 skipped
- `cargo deny --all-features check` advisories / bans / licenses / sources all ok

## What's deferred / TODO

### Explicitly out of scope (no code, may be revisited)

| Item | Why deferred |
|---|---|
| `[gcp]` cluster config block | ADC handles source discovery via env vars and well-known paths. Add only if a concrete knob is needed. |
| Token-mint metrics + trace spans | Out of scope; lambda has none either. Revisit if a signal (cache hit ratio, mint-failure rate) becomes operationally critical. |
| `restate dp update` CLI subcommand | Reconfigure via `dp register --force`, matching Lambda. |
| REST PATCH path to mutate `auth` | Same as above. `UpdateDeploymentRequest::Http.auth` field removed entirely. |
| Audience whitespace + SA-email regex validation | Cut as input-hygiene-not-capability. Typos surface as Cloud Run 401 on first invocation. |
| Cloud Run inbound `K_SERVICE` checks | Restate only emits tokens; Cloud Run handles inbound enforcement. |
| ADC source rotation invalidation | Tokens stay cached up to expiry-minus-60s regardless of underlying ADC source change. |
| Tightening Lambda's own `assume_role_arn` two-state shape | Pre-existing limitation, separate change. |

### Verified by reasoning, not by automated test

| Item | Status |
|---|---|
| H1 fix (HTTP update preserves auth) | Code is correct by parity with the Lambda `assume_role_arn.or_else()` merge at `registry/mod.rs:365`. No registry-layer integration test added (trait scaffolding is heavy). Recommend adding when next touching that file. |
| `dp register --force` clearing auth via re-register | Relies on existing overwrite semantics. Not exercised by an integration test. |
| Authorized-user ADC fails with a clear error when minting ID tokens without impersonation | Documented in release notes; runtime check only. |
| External-account / workload-identity-federation source minting | Delegated to `google-cloud-auth`; not exercised by Restate's tests. |

### Code-review items NOT addressed (intentionally)

From the main..HEAD reviewer (one CRITICAL=none, three HIGH=fixed, six MEDIUM, six LOW):

| ID | Status | Note |
|---|---|---|
| H1 | fixed | PATCH preserves auth |
| H2 | fixed | builder lock removed |
| H3 | fixed | `.expect()` → proper error |
| M1 | deferred | thundering-herd on cache miss is intentional; one comment added at the relevant site (not a code change, captured in the design notes) |
| M2 | deferred | clock skew vs JWT exp: failure mode is Cloud Run 401, observable, not silent |
| M3 | obsoleted | update auth field removed |
| M4 | obsoleted | `is_valid_sa_email` helper deleted |
| M5 | fixed | dead `Method` import removed |
| M6 | fixed | audience derivation consolidated in `restate_types::deployment::derive_audience` |
| M7 | accepted | dual reqwest versions via google-cloud-auth; `cargo deny` clean |
| L1 | fixed | `SourceKind` enum removed |
| L2 | wontfix | `seed_for_test` panic message clarity, dev-experience nit |
| L3 | accepted | per-insert HashMap clone in `rcu`; fine for deployment counts in tens |
| L4 | accepted | loopback IP list intentionally conservative (no CGNAT, no link-local v4) |
| L5 | wontfix | release-notes section ordering, cosmetic |
| L6 | accepted | `pub use` interleaving with cfg-gated impl; rustfmt has settled on current layout |

### Things to verify before merging to main

1. Rebase against main; the workspace has moved since branching.
2. `cargo hakari generate` and commit any workspace-hack delta.
3. Confirm cargo deny config doesn't need a new license exception for any of google-cloud-auth's transitive deps (current run was clean).
4. Manual smoke test against a real Cloud Run service: register a deployment, observe successful invocation, inspect the `X-Serverless-Authorization` header on the wire (and confirm Cloud Run is stripping it before the container so any customer `Authorization` reaches the workload). Especially useful given the absence of an end-to-end test against a real metadata server.
5. The H1 fix changes the persistence semantics for HTTP updates — if any in-flight ops automation relies on the previous (broken) behavior of HTTP updates clearing auth, surface that in the PR description.

## Spec → code traceability

13 numbered requirements after cuts. All are exercised by code + tests:

| REQ | Implementation | Test |
|---|---|---|
| REQ-DEP-01..03 (HttpAuth shape) | `crates/types/src/deployment.rs` | `auth_field_round_trips` |
| REQ-DEP-04..06 (mint paths, audience verbatim) | `crates/service-client/src/gcp.rs::mint_via_sdk` | `bearer_uses_explicit_audience_when_provided` |
| REQ-DEP-07 (audience derivation) | `restate_types::deployment::derive_audience` | gcp.rs unit tests (6) |
| REQ-AUTH-01 (X-Serverless-Authorization bearer) | `crates/service-client/src/lib.rs` ServiceClient::call | `bearer_attached_with_derived_audience` |
| REQ-AUTH-02 (customer Authorization passes through alongside the minted XSA) | same | `customer_authorization_passes_through_alongside_minted_xsa` |
| REQ-AUTH-03 (discovery + invocation) | dispatch path in ServiceClient::call | integration test path is shared |
| REQ-AUTH-04 (no fallback on failure) | `ServiceClientError::GcpAuth` early-returns from call | `mint_failure_does_not_send_unauthenticated_request` |
| REQ-REST-01..03 (REST shapes) | `crates/admin-rest-model/src/deployments.rs` | covered by handler-level smoke |
| REQ-CLI-01 (register flags) | `cli/src/commands/deployments/register.rs` | manual; no in-tree CLI test infrastructure for this |
| REQ-VAL-01 (https-or-loopback) | `validate_http_auth` | 3 unit tests |
| REQ-VAL-02 (dual-Authorization reject) | same | 2 unit tests |
| REQ-PERSIST-01 (serde backfill) | `crates/types/src/schema/deployment.rs::serde_hacks` | `can_deserialise_without_auth_field` |
