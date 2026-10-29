# Duration settings accept an unquoted `0`

## Bug Fix

### What Changed

Duration settings now accept `0` written as a plain number: unquoted in the configuration file
(`snapshot-interval = 0`) and in environment variables
(`RESTATE_WORKER__SNAPSHOTS__SNAPSHOT_INTERVAL=0`). Previously, `0` only worked as a quoted
string (`"0"` or `"0s"`), and the server refused to start with an "invalid type" error otherwise.

### Impact on Users

- Configurations that use `"0"` or `"0s"` are not affected.
- Settings that cannot be zero still reject `0`, now with the error "duration cannot be zero".
- Other numbers without a unit, such as `60`, are still rejected. Write `"60s"` instead.
