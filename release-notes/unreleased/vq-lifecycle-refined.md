# Vqueue entries record their lifecycle more precisely

## Bug Fix

Two values a vqueue entry records could mislead, as `sys_vqueue_entry_status` and `sys_vqueues`
show them:

- **The status of a running invocation, and of one resumed or woken up.** Paused and suspended
  entries keep their status, and resuming or waking them up restores it. But an invocation that
  once backed off, or was yielded, kept `backing-off` or `yielded` while it ran again, so pausing
  or suspending it carried that status, and it came back showing `backing-off` or `yielded`
  although it was simply ready to run. Every run now records `started`.
- **When a completed invocation's entry finished.** The entry recorded the time of the record that
  completed the invocation. That record can come from a node whose clock is behind, so an entry
  could appear to finish before it was created, or before the invocation's own `completed_at`.
  The entry now finishes at the invocation's `completed_at`, which never comes before its last
  modification.

A paused or suspended invocation still comes back with the status it had: one paused while
backing off resumes backing off, its pending retry included.

Only entries changing afterwards record the new values; existing entries keep theirs until their
next change.

### Enabling it

Leaders running v1.9.0 or newer turn it on by themselves once every worker node in the cluster runs
v1.9.0 or newer. To turn it on earlier, upgrade all nodes to v1.8.2 or newer, then enable:

```toml
experimental-enable-vq-lifecycle-refined = true
```

A partition turns it on the next time a leader is elected on a node that has the option, for
example when its current leader's node restarts with it.

- Once a partition has turned it on, it stays on, even if the option is removed.
- Once enabled, you **cannot** roll back to a Restate-server version older than v1.8.2.
