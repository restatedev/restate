# Partition followers read the log ahead in larger batches

## Behavioral Change

### What Changed

Partition replicas that are not leading (followers, and replicas catching up after a restart or
after joining) read the partition's log from log servers. They now read further ahead and in larger
requests:

- `bifrost.replicated-loglet.readahead-records` defaults to 512 records, up from 20.
- The new option `bifrost.replicated-loglet.readahead-bytes` (default 1 MiB) limits the memory of
  the records each replica has read ahead. A replica reads ahead until it holds 512 records or
  1 MiB, whichever comes first. The limit can be exceeded by one record, so larger records are
  still read.
- `bifrost.replicated-loglet.read-batch-size` defaults to 128 KiB, up from 32 KiB.

### Why This Matters

A follower reads the log one request at a time, and each request returned at most 20 records.
Under high write load followers fell behind their leaders, and replicas needed a long time to catch
up after a restart or when nodes joined the cluster. Reading more records per request cuts the
number of round trips a follower needs to replay the log.

### Impact on Users

- Followers can keep up with higher write rates, and replicas can catch up faster after restarts
  and rebalancing.
- Each replica on a node can hold up to about 1 MiB of read-ahead records, for example 32 MiB for a
  node with 32 replicas that are all catching up. Before, this memory was bounded only by
  20 records per replica.
- Log servers receive fewer, larger read requests.

### Migration Guidance

No action is needed. To restore the previous record and request limits:

```toml
[bifrost.replicated-loglet]
readahead-records = 20
read-batch-size = "32 KiB"
```

Read-ahead memory was not limited before; `readahead-bytes` now caps it. If your records are
large and replicas should keep more of them read ahead, raise `readahead-bytes`.
