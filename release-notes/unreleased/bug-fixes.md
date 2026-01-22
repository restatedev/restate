# Release Notes: Bug Fixes

## Bug Fixes

### Kafka

**Kafka `group.id` persistence fixed**

The `group.id` option configured in a Kafka cluster's `additional_options` is now correctly persisted to subscription metadata. Previously, the value was validated but not stored, which could cause issues when inspecting subscriptions or if cluster configuration changed.

- [#3919](https://github.com/restatedev/restate/pull/3919)

### Invocations

**Restart-as-new source tracking**

Restarted invocations now correctly show their origin. The `sys_invocation` table displays `invoked_by = 'restart_as_new'` and the `restarted_from` column contains the original invocation ID. Previously, restarted invocations were incorrectly marked as originating from `ingress`.

- [#3885](https://github.com/restatedev/restate/pull/3885)

**Invoker crash handling**

Improved handling of unexpected invoker crashes to prevent panics and incorrect state transitions. Retry timers are now properly fenced to avoid race conditions.

- [#4102](https://github.com/restatedev/restate/issues/4102)
- [#4085](https://github.com/restatedev/restate/issues/4085)

### Memory

**Memory leak in batch sender fixed**

Fixed a memory leak in the internal batch sender component.

- [#4124](https://github.com/restatedev/restate/pull/4124)

### CLI

**Query quote fix**

Fixed a misplaced quote in CLI SQL queries that could cause syntax errors.

- [#4116](https://github.com/restatedev/restate/pull/4116)

**Active invocations query fix**

Fixed queries for active invocations on a deployment that could return incorrect results.

- [#4115](https://github.com/restatedev/restate/pull/4115)

### UI

**Magic links fix**

Fixed an issue where magic links in the UI server were not working correctly.

- [#3946](https://github.com/restatedev/restate/pull/3946)

### Metadata

**H2 protocol errors now retryable**

H2 protocol errors in the metadata store client are now treated as retryable, improving resilience during transient network issues.

- [#4165](https://github.com/restatedev/restate/pull/4165)
