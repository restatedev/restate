# SQL queries keep node-to-node responses within the message size limit

## Bug Fix

### What Changed

When a SQL query reads a partition on another node, that node now keeps each
response within `networking.message-size-limit`. Before, a response holding many
large rows, such as big state values or journal entries, could exceed the limit.
The receiving node rejected it and dropped its connection to the sending node,
which failed the query and any other query reading from that node at the time.

Large results now arrive in more, smaller responses. A single row larger than
the limit still can't be sent: the query then fails with an error that names
`networking.message-size-limit`.

### Why This Matters

Queries over tables with large values, such as `state`, `sys_journal` or
`sys_promise`, work across nodes, and one query can no longer break the
connection other queries are using.

### Impact on Users

No configuration changes are required. To read single rows larger than the
limit, raise `networking.message-size-limit` on all nodes. During a rolling
upgrade, nodes that aren't upgraded yet can still send oversized responses.
