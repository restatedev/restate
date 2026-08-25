# Correct keyed invocation filters in the CLI

## Bug Fix

Batch invocation commands now correctly interpret two-component targets such as
`MyObject/myKey`. The CLI matches all handlers for the keyed service while still treating
`MyService/myHandler` as an exact service invocation target.

This fixes keyed target selection for `cancel`, `kill`, `pause`, `resume`, `purge`, and
`restart-as-new`. No configuration or migration changes are required.

Related issue: [#3752](https://github.com/restatedev/restate/issues/3752)
