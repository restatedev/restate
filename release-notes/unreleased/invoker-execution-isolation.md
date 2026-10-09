# Isolate invocation execution from partition processing

## Improvement

Heavy invocation traffic, including large lazy-state responses, now runs separately
from partition processing. This reduces interference with processing committed
records when service communication is busy.

Servers use one additional thread per partition they lead. No configuration changes
are required.

Unexpected invoker panics are reported as partition failures so the affected
partition can recover. Cleaning up an invoker also avoids delaying other
partitions while waiting for its background work to stop.
