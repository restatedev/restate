# Prevent vqueue refills from skipping queued invocations

## Bug Fix

### What Changed

Vqueue refills no longer advance their storage cursor beyond the last entry actually read from a
full storage batch when merging concurrent enqueue or removal notifications.

### Why This Matters

Previously, cancelling an invocation later in a vqueue while an asynchronous refill was in progress
could move the refill cursor past queued invocations that had not been read yet. Those invocations
could remain pending until the partition scheduler restarted.

### Impact on Users

Vqueues continue refilling from the last storage entry whose preceding range is known to be fully
covered. No configuration or data migration is required.
