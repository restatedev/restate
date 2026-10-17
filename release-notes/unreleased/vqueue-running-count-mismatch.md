# Release Notes: Leader takeover survives a vqueue running-count mismatch

## Bug Fix

### What Changed
When a partition processor becomes leader (or resumes a paused virtual queue) and a virtual
queue's stored running-entry count disagrees with the entries actually in its running stage,
the scheduler now logs the mismatch with the vqueue id and continues with the queue's inbox.
Previously it panicked (`vqueue has no running items but its metadata says it has N`), which
shut the whole node down after the 60 s graceful-shutdown timeout and failed over every
partition that node was leading.

### Impact on Users
- A node no longer restarts because one virtual queue's bookkeeping is off by one.
- Operators see an error-level log line naming the affected vqueue instead, which is the
  signal to report the inconsistency.
