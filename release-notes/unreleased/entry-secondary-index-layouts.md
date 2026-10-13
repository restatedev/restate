# Release Notes: Experimental entry secondary-index layouts

## Behavioral Change

### What Changed

Entry secondary indexes now provide transition-time and next-transition-time
access paths for three groupings: stage, stage and service, and virtual-object
identity and stage. Virtual-object identity includes service name, scope, and
object key.

Transition-time indexes sort newest first. Next-transition-time indexes sort
earliest first, with sequence numbers breaking same-second ties before the
canonical entry ID. Status is no longer part of these index keys, so status-only
updates do not rewrite secondary-index entries.

### Migration Guidance

These layouts are incompatible with experimental secondary-index data written
using the previous layouts. Index backfill and rebuilding are not implemented.
Use a fresh partition store when evaluating this revision with
`common.experimental-enable-indexes-v1` enabled; existing experimental index data
must not be treated as a complete or compatible inventory.
