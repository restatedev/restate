# Release Notes for PR #3951: DynamoDB Metadata Store Provider

## New Feature

### What Changed

Restate now supports using **Amazon DynamoDB** as a metadata store provider, as an alternative to the built-in Raft-based replicated metadata server.

### Why This Matters

- **Serverless deployments**: Run Restate without the `metadata-server` role, simplifying cluster topology
- **AWS-native integration**: Leverage DynamoDB's managed infrastructure, automatic scaling, and high availability
- **Simplified operations**: No need to manage Raft consensus for metadata; DynamoDB handles replication and durability
- **Multi-cluster support**: Share a single DynamoDB table across multiple Restate clusters using key prefixes

### Impact on Users

- **New optional feature**: Existing deployments are unaffected; this is an opt-in alternative to the replicated metadata server
- **Manual table creation required**: The DynamoDB table must be created before starting Restate

### Configuration

**1. Create the DynamoDB table:**

```bash
aws dynamodb create-table \
    --table-name metadata \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

The table must have a string partition key named `pk`. Choose an appropriate table name and billing mode for your setup.

**2. Configure Restate to use DynamoDB:**

```toml
roles = [
    "http-ingress",
    "admin",
    "worker",
    "log-server",
]
# Note: "metadata-server" role is removed

[metadata-client]
type = "dynamo-db"
table = "metadata"

# Authentication: use EITHER aws-profile OR explicit credentials
aws-profile = "my-profile"
# aws-access-key-id = "..."
# aws-secret-access-key = "..."
# aws-session-token = "..."        # For STS temporary credentials

# Optional settings
aws-region = "eu-central-1"        # Required unless inferred from profile/environment
# key-prefix = "prod-cluster/"     # Namespace for multi-cluster setups
# aws-endpoint-url = "http://localhost:8000"  # For local testing
```

**Required IAM permissions:**
- `dynamodb:GetItem```
- `dynamodb:PutItem`
- `dynamodb:DeleteItem`

### Related Issues

- [#3551](https://github.com/restatedev/restate/issues/3551): Add DynamoDB-based metadata store integration
