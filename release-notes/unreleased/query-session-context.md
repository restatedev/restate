# Query session correlation

## New Feature

SQL query responses now include `X-Restate-Query-Session-Id`, allowing clients to
correlate a request with server query logs. The HTTP endpoint also returns this
header on query errors after a session has been created.

Query execution and stream-failure logs also include a numeric `query_ts` alongside
the session ID, distinguishing individual executions within a session.

Existing result formats are unchanged.
