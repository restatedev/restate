# Who reads and how fast in `bifrost_read_streams`

## New Feature

The `bifrost_read_streams` table has four new columns:

- `name` tells who reads the log, e.g. `pp-3` for the processor of partition 3.
- `role` tells what the reader does: for a partition processor, `leader`, `becoming-leader`,
  `candidate` or `follower`. It changes when the processor's leadership does.
- `records_read` and `bytes_read` count the records each read stream has delivered and their size
  as the log stores them, across the log's segments. They start at zero when a stream is created,
  for example when a partition processor starts. Sampling them shows how fast each replica reads
  its log.
