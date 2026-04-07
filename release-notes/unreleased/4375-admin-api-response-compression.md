# Release Notes for Issue #4375: Admin API response compression

## New Feature

### What Changed
The Admin API now supports transparent response compression using gzip, brotli, and zstd.
Clients that send an `Accept-Encoding` header will receive compressed responses automatically.

### Why This Matters
Large API responses (e.g., listing many deployments or services) can now be significantly smaller
over the wire, reducing bandwidth usage and improving response times for clients.

### Impact on Users
- No action required — compression is negotiated transparently via standard HTTP content encoding.
- Clients that already send `Accept-Encoding` (most HTTP clients and browsers do by default) will
  automatically benefit from smaller responses.
- Clients that do not send `Accept-Encoding` will continue to receive uncompressed responses.

### Related Issues
- Issue #4375
