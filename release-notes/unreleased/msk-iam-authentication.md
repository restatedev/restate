## AWS MSK IAM Authentication Support

Add support for AWS MSK IAM authentication via SASL/OAUTHBEARER mechanism. This allows connecting to AWS Managed Streaming for Apache Kafka (MSK) clusters that use IAM-based access control.

### Configuration

To use MSK IAM authentication, configure your Kafka subscription with the following options in `additional_options`:

```yaml
additional_options:
  security.protocol: SASL_SSL
  sasl.mechanisms: OAUTHBEARER
  sasl.oauthbearer.config: "provider=msk-iam,region=us-east-1"
```

The `sasl.oauthbearer.config` field supports the following configuration:
- `provider=msk-iam` - **(Required)** Specifies the OAUTHBEARER token provider to use
- `region=<aws-region>` - The AWS region where your MSK cluster is located (default: `us-east-1`)
- `profile=<profile-name>` - (Optional) The AWS credentials profile to use

Example with profile:
```yaml
additional_options:
  security.protocol: SASL_SSL
  sasl.mechanisms: OAUTHBEARER
  sasl.oauthbearer.config: "provider=msk-iam,region=us-west-2,profile=my-aws-profile"
```

### Supported Providers

Currently, the following OAUTHBEARER providers are supported:
- `msk-iam` - AWS MSK IAM authentication using SIGv4 signed tokens

The provider-based architecture allows for future extensions to support other OAUTHBEARER-based authentication mechanisms (e.g., Confluent Cloud, Azure Event Hubs).

### AWS Credentials

The `msk-iam` provider uses the AWS default credential chain, which supports:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM instance profiles (for EC2/ECS/EKS)
- Web Identity Token (for EKS Pod Identity/IRSA)

### Backward compatibility

This is opt-in. Subscriptions that do not set `sasl.mechanisms=OAUTHBEARER` are
unaffected — the new token callback is only invoked by librdkafka when
OAUTHBEARER is configured. Existing PLAIN/SCRAM/no-auth Kafka consumers behave
exactly as before.