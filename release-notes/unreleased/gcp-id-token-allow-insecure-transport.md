# Release Notes: Google ID-token auth over plain HTTP on request

## New Feature

### What Changed
HTTP deployments using Google ID-token authentication can set `auth.allow_insecure_transport`
(CLI: `--gcp-allow-insecure-transport`) to register a plain-http URL on a public hostname, for
example an endpoint reached through Google Private Service Connect. Without it, registration
still requires https unless the host is a loopback or private IP address. With it, tokens are
sent in cleartext, so use it only on networks you trust.
