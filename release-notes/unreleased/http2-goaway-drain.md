# Preserve active HTTP/2 streams during graceful GOAWAY

## Bug Fix

A graceful GOAWAY received before a new service request is sent now retires the
connection from the pool without cancelling streams already in progress. The
unsent request is retried on another connection with its body intact.

Previously, a readiness error on the new request cancelled the shared connection
and could interrupt unrelated service invocations. Hard connection failures still
propagate normally; requests already sent are not automatically replayed by this
change. No configuration or migration is required.
