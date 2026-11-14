# restatectl reads responses of any size

## Behavioral Change

### What Changed

`restatectl` no longer rejects responses from the cluster that are larger than
about 32 MiB. It accepts any response a server sends, and only allocates memory
for each response as it arrives. Before, a large result, for example a
`restatectl sql` query over many large rows, failed once a single response
passed that size.

### Impact on Users

No configuration changes are required. Server-side limits are unchanged. To cap
the size of responses `restatectl` accepts, pass the hidden
`--message-size-limit` option.
