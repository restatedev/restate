# Lower memory use for invocation joins in query engine v2

## Bug Fix

Fixes excessive memory use in the experimental query engine v2 when joining
invocation history with live invocation state, including service-status summaries
in the Web UI. These queries can now build their join on the smaller live-state
table instead of retaining the invocation history in memory.

No configuration change is required.
