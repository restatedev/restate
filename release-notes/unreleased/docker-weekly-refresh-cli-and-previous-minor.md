# Release Notes: Weekly Security Refreshes for More Docker Images

## New Feature

### What Changed
The weekly security refresh, which rebuilds published images on an up-to-date base OS without changing the Restate binaries, now covers:
- the `restatedev/restate-cli` image, in addition to `restatedev/restate`
- the latest patch release of the previous minor version (e.g. 1.7.x), in addition to the latest release

### Why This Matters
OS security fixes now reach the CLI image and users on the previous minor version without waiting for a new Restate release.

### Impact on Users
- No action is needed if you use a version tag such as `1.8.1` or `1.8`; pulling it again gets the refreshed image.
- Each refresh is also published under a date-suffixed tag, such as `1.8.1-20261026`, if you need an immutable reference.
- If you pin images by digest, you will not receive refreshes until you update the digest.
