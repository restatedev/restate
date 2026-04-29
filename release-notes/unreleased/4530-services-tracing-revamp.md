# Release Notes for Issue #4530: Services tracing revamp

## Behavioral Change

### What Changed
The shape of services tracing has changed:

- The single long-running invocation span is gone. In its place, the partition processor emits three spans per invocation: one at start, one at the end, and one per attempt (emitted by the invoker). 
- These spans will be emitted **as soon as they're ready**, giving real-time feedback of the running invocations in your tracing infrastructure. No more spans emitted just at the end of the invocation.
- Per-command spans are gone. Commands are now recorded as events on the relevant attempt span.
- The OTel `Resource` rewriting on exported spans has been removed. Spans are now exported with the actual process resource (Restate), as the OTel spec requires.

Ingress spans are unchanged.

### Impact
- Dashboards, alerts, or queries keyed on the old long-running invocation span name or on per-command spans will need to be updated.
- Traces now reflect the real process emitting them, which may change how spans are grouped in your tracing backend.
