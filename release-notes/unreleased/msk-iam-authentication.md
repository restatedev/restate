The Kafka ingress now supports **AWS MSK IAM authentication** for subscriptions,
using SASL/OAUTHBEARER with SigV4-signed tokens.

To use it, configure the subscription's Kafka client with:

```
security.protocol = SASL_SSL
sasl.mechanisms    = OAUTHBEARER
sasl.oauthbearer.config = provider=msk-iam,region=<aws-region>,profile=<aws-profile>
```

`region` defaults to the ambient AWS region when omitted, and `profile` is
optional. Tokens are generated at runtime from the standard AWS credential
chain, so no static secrets are stored.

MSK IAM support is compiled into the default build. The custom OAUTHBEARER token
callback is installed **only** for subscriptions that select
`provider=msk-iam`; all other subscriptions fall back to librdkafka's built-in
OAUTHBEARER handling, so existing OIDC (e.g. Confluent) authentication continues
to work unchanged.
