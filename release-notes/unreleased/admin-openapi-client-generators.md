# Admin API OpenAPI document works with standard client generators

## Behavioral Change

The OpenAPI document served at the Admin API's `/openapi` endpoint can now be
fed to standard client generators, such as openapi-generator, to produce
working clients.

Deployment responses returned by `GET /deployments`,
`GET /deployments/{deployment}` and `PATCH /deployments/{deployment}` now carry a
`type` field with the value `http` or `lambda`. The field is additive, so
existing clients keep working. Clients generated from the previous document
should be regenerated.
