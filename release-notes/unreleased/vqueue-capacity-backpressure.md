# Improve recovery from invocation-capacity saturation

## Bug Fix

### What Changed

Fixed excessive CPU usage and reduced invocation throughput when many distinct
virtual queues are waiting for invoker capacity. Scheduling now leaves more time
for processing invocation results and making progress on existing work.

### Impact on Users

Workloads with many distinct virtual queues should recover more effectively from
capacity saturation instead of spending increasing CPU on waiting work. No
configuration changes or migration steps are required.
