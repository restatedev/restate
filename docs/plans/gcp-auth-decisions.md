# GCP Auth: Decisions and Open TODOs

Living document. Captures decisions made during implementation and
TODOs deferred from the vertical-slice path. Update as you go.

## Strategy

**Vertical slice first.** Goal: a single working e2e test exercising
token mint + bearer attachment against a local emulator. Everything
else (CLI polish, validation rule completeness, observability metrics,
schema-evolution tests) builds on top.

Order of work for the slice:

1. Add `google-cloud-auth` dependency with `idtoken` feature.
2. Token-mint client (`crates/service-client/src/gcp.rs`) with the
   minimal API: `mint(impersonate_sa, audience) -> id_token`.
3. `HttpAuth::GoogleIdToken` schema variant on `DeploymentType::Http`
   (skip serde_hacks complexity for v0; just `#[serde(default)]`).
4. Endpoint dispatch: attach bearer when auth is set.
5. One integration test using `httptest` (or similar) that mocks the
   GCE metadata server's `identity` endpoint and asserts the
   bearer reaches an upstream HTTP test server.
6. STOP — everything compiles, the slice runs, commit.

After that, fill in REST model, schema registry merge, CLI, validation,
observability, docs, release notes.

## Decisions

### D1: TLS provider conflict
`google-cloud-auth` defaults to `default-rustls-provider` (aws-lc-rs).
Restate already pulls aws-lc-rs via AWS SDKs. Decision: keep defaults
on, but watch for runtime "no default crypto provider" panics. If they
appear, install the provider explicitly at node startup before any
service-client construction.

### D2: HTTP client interop
`google-cloud-auth` uses reqwest. Restate's service-client uses hyper.
For v0, accept the additional reqwest+rustls dep. Hyper transition is
out of scope.

### D3: Cache key sentinel
Cache key is `(Option<String>, String)`. `None` for ambient ADC
(REQ-CACHE-02). No string-sentinel needed.

### D4: serde_hacks for HttpAuth
Not needed in v0. The `compression` precedent in `serde_hacks`
(`crates/types/src/schema/deployment.rs:244`) uses `#[serde(default,
skip_serializing_if = "Option::is_none")]` directly on the field — no
separate parse-and-convert module. Mirror that pattern.

### D5: Provider-typed enum on REST vs schema (SUPERSEDED)
Original decision: both REST model and persisted schema use the same
`HttpAuth` enum externally-tagged (REQ-DEP-02), avoiding parallel
types.

Superseded by review feedback. The REST surface and the persisted
schema evolve under different compatibility rules (wire under REST
versioning, persisted under storage-format evolution) and sharing a
single struct couples that evolution. The project has already paid for
the conflation in similar shapes elsewhere. Current decision: the
admin-rest-model crate defines an independent wire-side copy of
`HttpAuth` and `GoogleIdTokenAuth`; `restate-types` keeps the persisted
copy in `crates/types/src/deployment/http_auth.rs`. The REST handler
converts between them at the boundary via `From` impls.

### D6: Full Lambda parity, no `dp update` CLI, no REST tri-state
Reverses two earlier intermediate decisions. The user-facing and
REST shapes both match Lambda's `assume_role_arn` exactly:

- `auth: Option<HttpAuth>` on register, set at registration time.
- `auth: Option<HttpAuth>` on update, two-state (absent = preserve,
  present = replace). No path to clear `auth` from an update payload.
- Reconfigure by re-registering with `--force` (and no auth flags to
  remove auth, or with new flag values to change it). Same workflow
  Lambda has used since day one.

Dropped requirements:

- REQ-CLI-04 (full `dp update` flag surface)
- REQ-CLI-04a (CLI-side merge-fetch behavior)
- REQ-CLI-05 (`--no-id-token` vs positive-flag conflict guard)
- REQ-CLI-08 (the new subcommand itself)
- REQ-CLI-09's `dp update` half
- REQ-REST-02a (`Option<Option<HttpAuth>>` serde recipe)
- REQ-REST-02b (three-way merge logic in the schema registry)
- REQ-TEST-06 (tri-state round-trip tests)
- Section 10 in full: REQ-OBS-01..04 (token-mint metrics + spans).
  Observability for the token-mint path is out of scope for v1. If a
  specific signal turns out to be critical to monitor (e.g. cache
  hit ratio degrading because tokens churn faster than expected),
  add it then.

REQ-REST-02 reduces to "two-state Option<HttpAuth>, same as Lambda".

Why: the tri-state was introduced by codex's review to support a
`--no-id-token` CLI flag. The flag's already gone (previous D6
revision); the REST tri-state's justification went with it. Restate
has no other tri-state PATCH endpoints, so keeping one here would set
a new precedent for a use case (REST-only clear) we have no real
demand for. Typical installations carry a handful of deployments;
re-register-with-force is the right tool for the job.

If a future use case appears ("REST automation needs to clear `auth`
without re-running discovery on N>>handful deployments"), we tighten
both Lambda and HTTP at once.

## Open TODOs

These do NOT block the e2e slice. Track here so they aren't lost.

### T1: Hakari regeneration
After adding `google-cloud-auth` to a workspace member, run
`cargo hakari generate` and commit any workspace-hack changes.

### T2: Cargo deny check
`google-cloud-auth` and its transitive deps may have licenses or
advisories not yet in the project's allowlist. Run
`cargo deny --all-features check` and update `deny.toml` if needed.
Be conservative: prefer adding specific exceptions over broad
license rules.

### T3: All-three-call-sites GCP wiring (REQ-CACHE-01)
The Lambda cache mode has 3 call sites:
- `crates/node/src/roles/admin.rs:102-107` (admin/discovery, None)
- `crates/invoker-impl/src/lib.rs:318-319` (primary invoker, Unbounded)
- `crates/invoker-impl/src/lib.rs:~2179` (secondary invoker, None per
  codex; verify at impl time)

Mirror the same wiring for the GCP token client.

### T5: Validation rules suite
REQ-VAL-01..07 each need handler-side code paths and unit tests. Do
these after the slice works.

### T7: dp describe display rows
REQ-CLI-05/06. Adds two rows when auth is set, omits when not.
Already implemented; this entry kept for traceability.

### T8: SA email regex on CLI side (REQ-CLI-07)
CLI rejects GCP auth flags for Lambda ARN endpoints before REST.
Already implemented for `dp register`; no longer needs to cover
`dp update` since that subcommand was dropped (D6).

### T9: Cloud Run Invoker role doc (REQ-DOC-03)
Release note must mention both `serviceAccountOpenIdTokenCreator` (for
impersonation) AND `roles/run.invoker` (for the principal whose
identity ends up in the token to actually be allowed to invoke).

### T10: Authorized user ADC behavior (REQ-TOK-06)
For ambient ADC with gcloud user creds, the crate may or may not
support minting ID tokens directly. Verify behavior; if it errors,
return our REQ-TOK-06-shaped error. If it works for arbitrary
audiences (it shouldn't, but verify), document the caveat.

### T11: `quota_project` plumbing (REQ-CFG-02)
`google-cloud-auth` may or may not expose a quota-project setter on
the idtoken Builder. If not, this requirement is deferred and
documented.

### T12: External account / WIF support (REQ-TOK-02)
Resolved: the `google-cloud-auth` crate's `idtoken::Builder` rejects
`external_account` (Workload Identity Federation) and `authorized_user`
(gcloud user creds) ADC sources at runtime. Both work only when paired
with `--impersonate-service-account`, which routes minting through the
IAM Credentials `generateIdToken` API on the target SA. An ambient WIF
path (mint ID tokens directly from a federated external account
without impersonation) is deferred; it could be added later via a
custom mint path if a customer needs it.

### T13: Log redaction
REQ-TOK-07 forbids logging token contents at any level and forbids
logging audience or impersonate_service_account above DEBUG. Make
sure no `tracing::info!("...{audience}...")` style logs slip through
during implementation.

### T14: SDK quota / rate limits
`generateIdToken` has per-project rate limits. The 10s timeout
(REQ-TOK-08) and Unbounded cache should keep us under, but document
the assumption.

### D7: Slice excludes schema changes
Originally the slice was going to touch `DeploymentType::Http` with the
new `auth` field. Decision: defer schema changes to Phase B. The slice
exercises the token-mint + bearer-attach path by constructing
`Endpoint::Http` with `HttpAuth` directly in a test, which is faster
to land and still validates the most important part (audience
derivation + JWT minted + header attached).

## Resolved

(move items here when done)
