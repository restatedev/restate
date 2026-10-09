# Release Notes for Issue #5033: Prevent Bifrost read stream stalls

## Bug Fix

### What Changed

Bifrost readers now use log-chain metadata to continue reading through a repaired sealed segment even when the loglet's local tail view is stale.

### Impact on Users

No action is required. This prevents affected partition processors from stalling at a segment boundary until restart.

### Related Issues

- Issue #5033
