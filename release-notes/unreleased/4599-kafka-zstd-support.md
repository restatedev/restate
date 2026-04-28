# Release Notes for Issue #4599: Add zstd compression support for Kafka consumer

## New Feature

### What Changed
The Kafka consumer now supports zstd-compressed topics. Previously, only gzip, snappy,
and lz4 compression were supported. Consuming from a zstd-compressed topic would fail
with a `Decompression (codec 0x4) failed: Local: Not implemented` error.

### Why This Matters
zstd is a widely used Kafka compression codec. Restate now supports all four Kafka
compression codecs: gzip, snappy, lz4, and zstd.

### Impact on Users
- Kafka subscriptions to zstd-compressed topics now work out of the box.
- No configuration changes required.

### Related Issues
- Issue #4599