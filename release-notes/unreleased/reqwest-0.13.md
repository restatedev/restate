# Update HTTP client TLS verification

## Behavioral Change

Restate's CLI and developer tools now use Reqwest 0.13 with Rustls and platform
certificate verification. This replaces Reqwest 0.12's bundled WebPKI root
selection for these clients, so certificate trust follows the operating system's
trust configuration.

The HTTP OpenTelemetry exporter retains Reqwest 0.12 for compatibility with
OpenTelemetry 0.31. The service-invocation HTTP client uses its existing Hyper
integration.
