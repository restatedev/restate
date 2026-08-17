# Failure detector startup timing

## Bug Fix

The failure detector now resets its gossip timing when the node becomes ready. Time spent starting
node roles is no longer counted as missed gossip intervals or as time without received gossip,
preventing a starting node from incorrectly considering healthy peers dead or itself isolated.

No configuration or migration changes are required.

Related issue: [#5156](https://github.com/restatedev/restate/issues/5156).
