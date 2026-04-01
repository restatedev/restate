# Dial9 runtime telemetry

## New Feature

Restate can now be built with the `dial9` feature to record low-overhead Tokio runtime telemetry.
The default runtime and all managed partition runtimes write to one shared trace, allowing their
task scheduling, wakeups, and worker activity to be analyzed together. Linux builds also capture
CPU profiles and scheduler events.

Trace storage is configured under the common `dial9` section. `trace-dir` defaults to
`restate-data/dial9-traces`, `max-file-size` defaults to 10 MiB, and `max-total-size` defaults to
100 MiB for the shared trace. Dial9 initialization failures do not prevent Restate from starting;
the affected runtimes continue without instrumentation.

No migration is required. Existing builds are unchanged unless the `dial9` Cargo feature is
enabled.
