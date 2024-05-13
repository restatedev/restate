# Cloud clients

To generate a json schema for typify:

```bash
yq '.definitions = .components.schemas | del(.paths, .components) | .["$schema"] = "http://json-schema.org/draft-07/schema#" | .' ../restate-cloud/generated/schema/openapi.yaml > cli/src/clients/cloud/schema.json
```
