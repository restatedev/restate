# More responsive storage-backed SQL queries

## Bug Fix

### What Changed

Storage-backed SQL queries can continue making progress while another query is
waiting for its results to be consumed. Cancelling or abandoning a query also
releases its pending storage work more promptly.

Queries stop additional reads once their pushed-down limit is satisfied. If a
storage read fails, the query reports the error and discards its remaining
buffered rows.

### Impact on Users

No configuration changes are required.
