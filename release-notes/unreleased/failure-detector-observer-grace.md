# Failure detector observer grace after scheduling gaps

## Bug Fix

### What Changed

After the failure detector misses multiple gossip intervals, it now waits for a full failure-detection window of consecutive normal polls before using locally accumulated gossip age to mark peers dead.

The detector continues to exchange the actual gossip ages during this grace period. Fresh gossip and explicit failover messages continue to update peer state immediately, and terminally closed gossip connections remain an immediate failure signal.

### Why This Matters

A delayed observer previously charged every missed interval to every peer when it resumed. A sufficiently long local scheduling gap could therefore make that observer mark healthy, connected peers dead and publish a divergent cluster view.

The grace period prevents an observer from making age-based failure decisions until it has processed enough normal polls to re-establish a reliable observation window.

### Impact on Users

Clusters are less likely to experience unnecessary leadership and placement changes when a Restate server is temporarily overloaded or its failure detector is delayed.

After such a delay, detecting a genuinely silent peer through gossip age may take up to one additional failure-detection window. Direct failure evidence is unaffected.

### Migration Guidance

No action is required.
