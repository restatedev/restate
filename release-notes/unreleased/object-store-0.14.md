# Update cloud object storage clients

## Bug Fix

Snapshot storage and object-store metadata clients now use `object_store` 0.14.2.
This updates the XML parser to address RUSTSEC-2026-0194 and RUSTSEC-2026-0195,
fixes retries for S3 bulk-delete responses containing throttling or internal
errors, and corrects Azure's error classification for create-if-absent writes.

The upgrade also updates the HTTP client to Reqwest 0.13 and uses AWS-LC as the
default cryptographic provider for object-store cloud clients. Existing Restate
object-store configuration keys are unchanged.

DataFusion retains its separate, filesystem-only `object_store` 0.13 dependency;
it does not enable the older cloud client's XML parser.
