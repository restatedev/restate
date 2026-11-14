# More responsive SQL queries over partition storage

## Bug Fix

### What Changed

SQL queries that read partition storage, such as `sys_journal`,
`sys_invocation_status` or `state`, no longer hold storage threads while their
client is slow to read the results. Other queries keep making progress in the
meantime, and cancelling or abandoning a query releases its storage reads
promptly.

A query with a `LIMIT` stops reading storage once it has enough rows. If a
storage read fails, the query fails with that error; rows it read but hadn't
sent yet are dropped.

### Why This Matters

Before, a query whose client read results slowly kept storage threads busy that
partitions also use for their own reads.

### Impact on Users

No configuration changes are required.
