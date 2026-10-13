# Query session correlation

## New Feature

SQL query responses now include `X-Restate-Query-Session-Id`, allowing clients to
correlate a request with server query logs. The HTTP endpoint also returns this
header on query errors after a session has been created.

Existing result formats are unchanged.
