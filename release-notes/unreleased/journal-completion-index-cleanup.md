# Background cleanup of obsolete journal completion indexes

## Bug Fix

Restate now cleans up unused journal completion-index entries left behind by an
older journal deletion bug while partitions continue serving requests. Cleanup
resumes after restarts and preserves entries belonging to live journals.

Progress and failures are visible in `restate.cluster.migration_jobs` under
`jc_orphan_cleanup/1`. Existing background-migration throttling and disable
settings apply. No action is needed; partitions already marked as cleaned are
skipped.
