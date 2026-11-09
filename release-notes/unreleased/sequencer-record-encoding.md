# Reduced CPU work during replicated log writes

## Improvement

Replicated log writes reuse serialization work across replicas and retries,
reducing CPU work on the sequencer. Existing nodes remain wire-compatible;
no configuration changes or migration steps are required.
