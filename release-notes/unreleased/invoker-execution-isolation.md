# Isolate invocation execution from partition processing

## Improvement

Heavy invocation traffic, including large lazy-state responses, now runs separately
from partition processing. This reduces interference with processing committed
records when service communication is busy.

Servers use one additional thread per partition they lead. No configuration changes
are required.
