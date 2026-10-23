# Release Notes: Up-to-Date OS Trust Store in the Docker Image

## Bug Fix

### What Changed
The Docker image now installs Debian's current `ca-certificates` package instead of carrying a fixed copy of the trust store from an older build image. The weekly security refreshes of published images now update it as well.

### Why This Matters
Previous images, including 1.8.1, still trusted the emSign ECC Root CA - G3 and Comodo AAA Services roots, which Debian has removed after the emSign (eMudhra) intermediate keys were compromised. Kafka connections and `curl` in the image verify certificates against this trust store.

### Impact on Users
- No configuration changes are needed.
- Servers that present certificates chaining only to the removed roots will fail TLS verification for Kafka connections.
