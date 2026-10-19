# Release Notes for PR #5598: Drain in-flight Kafka records when a subscription is deleted

## Behavioral Change

### What Changed
Deleting a Kafka subscription used to stop its consumer immediately, which
could drop records that were already read from Kafka but not yet accepted by
Restate. Restate now stops reading new messages, waits up to 5 minutes for the
in-flight records to be accepted, and logs where every topic partition stopped:

```
Kafka partition drained, last offset ingested into Restate: <offset>
Kafka partition drained, no records were ingested since the partition was assigned
Kafka subscription drained, the last ingested offsets were logged per partition
Committed Kafka consumer group offsets of the draining subscription: [<topic>/<partition>: <offset>, ...]
```

The `Committed` line is logged for every commit while draining, including the
periodic auto commits, so a partition can appear in several of them. The last
line that mentions a partition shows its committed offset. A committed offset
is the next offset to read, so it is one higher than the last ingested offset.

Draining is best effort. If the server crashes, or if the drain does not
finish within 5 minutes (logged as `did not finish draining`), records can
still be dropped. A partition whose drain fails logs
`Draining the Kafka partition failed` together with its last ingested offset.
If a consumer group rebalance happens while draining, Restate may no longer be
able to store the offsets of the records it still accepts (logged as
`Failed to store the Kafka offset while draining`), in which case the committed
offset lags behind the ingested one.

Restarts caused by configuration changes, consumer group rebalances, and server
shutdown are unaffected.

### Why This Matters
This makes it possible to delete a subscription and hand its topics over to
another consumer, in particular when following the
[migration guide to the Kafka ingress integration](https://docs.restate.dev/services/integrations/kafka#migrating-from-the-old-kafka-support),
knowing where Restate stopped.

### Migration Guidance
When deleting a subscription in order to migrate its topics to the Kafka
ingress integration:

1. Delete the subscription and wait for the `Kafka subscription drained` log
   line on every Restate node.
2. For every partition that logged a `last offset ingested into Restate`,
   compare that offset + 1 with the committed offset in the last `Committed
   Kafka consumer group offsets` line that mentions the partition. If they
   match, the consumer group already points to the right offset.
3. If they do not match, the partition has no commit line, or the commit line
   reports an error for it, set the consumer group offset of that topic
   partition to **the logged ingested offset + 1** before starting the
   integration. For example, with the standard Kafka tooling:

   ```bash
   kafka-consumer-groups.sh --bootstrap-server <broker> --group <group> \
     --topic <topic>:<partition> --reset-offsets --to-offset <ingested offset + 1> --execute
   ```

   Partitions that logged `no records were ingested` can keep the consumer
   group's current offset. Partitions without any `drained` or `failed` line
   may have had records in flight when the drain was given up; starting from
   an older offset re-delivers those records, starting from a newer one skips
   them.

Starting the integration from an offset older than the one Restate ingested
re-ingests records Restate already accepted, resulting in duplicate
invocations.

### Related Issues
- PR #5598: Drain in-flight records when a subscription is deleted
