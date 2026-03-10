# Invoker metrics

## Dropped metrics
The following metric has been dropped:
- `restate.invoker.available_slots`

## Added metrics
Two new counter metrics have been added as replacements:
- `restate.invoker.concurrency_slots.acquired` (counter)
- `restate.invoker.concurrency_slots.released` (counter)

These counters make it easy to derive:
- Rate of slot acquisition and release
- Available slots: `restate.invoker.concurrency_limit - (restate.invoker.concurrency_slots.acquired - restate.invoker.concurrency_slots.released)`
