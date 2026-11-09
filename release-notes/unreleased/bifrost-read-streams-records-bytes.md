# Read progress in `bifrost_read_streams`

## New Feature

The `bifrost_read_streams` table has two new columns: `records_read`, the number of records each
read stream has delivered since it was created, and `bytes_read`, their size as the log stores
them. Both count across the log's segments and start at zero when a stream is created, for example
when a partition processor starts or changes leadership. Sampling them shows how fast each
replica reads its log, in records and bytes.
