# V2 query cleanup after connection loss

## Bug Fix

Workers now release V2 query tasks when the connection that installed them
closes, including tasks waiting for data or still being set up. Previously,
an active task could retain query resources after its coordinator connection
was lost.

Healthy queries waiting on a live connection are not given a new execution
time limit. No configuration change is required.
