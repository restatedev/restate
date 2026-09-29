# Upgrade the Admin API OpenAPI generator to utoipa 6

## Behavioral Change

The OpenAPI document served at the Admin API's `/openapi` endpoint is now
generated with utoipa 6. The set of endpoints and schemas is unchanged, and the
document remains OpenAPI 3.1.

Two cosmetic details of the generated document differ:

- Optional fields still use `oneOf` with a `null` alternative, but the concrete
  schema is now listed before `null` rather than after it.
- Responses without a description no longer carry an empty `description` field.

Clients generated from the previous document keep working. Regenerate clients
only if your tooling is sensitive to the ordering of `oneOf` alternatives.
