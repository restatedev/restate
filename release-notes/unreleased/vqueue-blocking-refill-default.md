# Use Blocking VQueue Refills by Default

## Behavioral Change

### What Changed

VQueue storage refills now run synchronously by default. The previous asynchronous refill path can
be restored with the experimental option:

```toml
experimental-enable-vqueues-async-refill = true
```

### Impact on Users

No configuration changes are required. Existing deployments will use synchronous VQueue refills
after upgrading.
