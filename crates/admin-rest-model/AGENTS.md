# Admin API models

These types are the Admin API wire format; their `utoipa::ToSchema` derives produce
an OpenAPI document that must work with standard client generators. Until utoipa
fixes these gaps:

- `#[serde(untagged)]` enums in **response** types, which generated clients must
  deserialize, MUST use newtype variants over named structs, each struct carrying a
  single-value `type` field, and declare
  `#[schema(discriminator(property_name = "type", mapping(...)))]` with mapping
  targets built by `crate::schema_ref::<Variant>()`. Reference:
  `DeploymentResponse` in `src/deployments.rs`
  (https://github.com/juhaku/utoipa/issues/1456). Request-body enums such as
  `RegisterDeploymentRequest` and `UpdateDeploymentRequest` stay untagged struct
  variants: clients only serialize them, and adding a `type` field would change
  the accepted request format.
- Enums with a variant-level `#[serde(untagged)]` MUST implement `PartialSchema`
  manually. Reference: `PatchDeploymentId` in `src/invocations.rs`
  (https://github.com/juhaku/utoipa/pull/1521).
- Fields typed as aliases or newtypes over primitives (e.g. `ServiceRevision`)
  MUST carry `#[schema(value_type = u32)]`; manual `ToSchema` impls MUST return
  their own name, never `String::name()`. Otherwise components named `u32` or
  `String` leak into the document.

After changing these types, validate the document per `crates/admin/AGENTS.md`.
