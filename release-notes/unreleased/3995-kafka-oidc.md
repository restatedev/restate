# Release Notes: Kafka SASL OAUTHBEARER/OIDC Authentication

## New Feature

### What Changed

The Kafka ingress now supports **SASL OAUTHBEARER** authentication, enabling OAuth 2.0/OpenID Connect token-based connections to managed Kafka services. This feature is enabled by default in all restate-server builds.

### Why This Matters

- **Managed Kafka services**: Connect to Confluent Cloud, Amazon MSK (with IAM authentication), Azure Event Hubs, and other providers that require OAuth-based authentication
- **Enhanced security**: Use short-lived OAuth tokens instead of long-lived credentials
- **Standard authentication**: OAUTHBEARER is the industry-standard mechanism for modern Kafka deployments

### Impact on Users

- **New capability**: Users can now configure Kafka subscriptions with OAUTHBEARER authentication
- **Enabled by default**: No special build flags needed; the feature is included in standard restate-server binaries

### Configuration

Configure SASL OAUTHBEARER via the `additional_options` field in your Kafka cluster configuration. These options are passed directly to librdkafka.

**Example for Confluent Cloud:**
```toml
[[ingress.kafka-clusters]]
name = "my-cluster"
brokers = ["SASL_SSL://pkc-xxxxxx.eu-central-1.aws.confluent.cloud:9092"]
"security.protocol"="SASL_SSL"
"sasl.mechanisms"="OAUTHBEARER"
"sasl.oauthbearer.method"="oidc"
"sasl.oauthbearer.client.id"="<your-client-id>"
"sasl.oauthbearer.client.secret"="<your-client-secret>"
"sasl.oauthbearer.token.endpoint.url"="<your-token-endpoint>"
"sasl.oauthbearer.scope"="kafka"
```

**Common OAUTHBEARER options:**

| Option | Description |
|--------|-------------|
| `security.protocol` | Set to `SASL_SSL` for encrypted connections |
| `sasl.mechanism` | Set to `OAUTHBEARER` |
| `sasl.oauthbearer.method` | Set to `oidc` for OIDC-based token retrieval |
| `sasl.oauthbearer.client.id` | OAuth client ID |
| `sasl.oauthbearer.client.secret` | OAuth client secret |
| `sasl.oauthbearer.token.endpoint.url` | OAuth token endpoint URL |
| `sasl.oauthbearer.scope` | OAuth scope (if required by provider) |

For the full list of available options, see the [librdkafka CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

### Related Issues

- [#3995](https://github.com/restatedev/restate/issues/3995): Introduce kafka-oidc feature and enable it by default
