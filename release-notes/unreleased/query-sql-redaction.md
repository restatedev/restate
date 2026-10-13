# Redacted SQL query diagnostics

## Behavioral Change

SQL execution logs replace literal values with `?` and include the query session ID.
Comments are removed; table and column names, aliases, and type parameters remain visible.
Queries that cannot be safely represented are omitted from SQL diagnostics.

Query-stream failure logs include the session ID and redacted query instead of the
original SQL and error text. Detailed errors remain available in the query response.
