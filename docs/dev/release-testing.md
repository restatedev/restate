# Release Testing Guide

This document describes the testing tasks that should be performed before releasing a new major/minor version of Restate.

## Overview

**Purpose:** Ensure quality, stability, and compatibility before releasing.

**When to start:** Only begin testing tasks once the release has been frozen, except for well-isolated features that can be tested independently.

**Documentation check:** When testing new features, always verify that the documentation is up-to-date and that you can navigate the feature easily. If not, update the documentation before release.

## Automated Test Suites

### CI Pipeline

The CI pipeline ([`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)) runs automatically on PRs and pushes to main/release branches. It includes:

- **Build and unit tests** - Core functionality verification
- **SDK integration tests** - Java, Python, Go, TypeScript, and Rust SDKs
- **E2E tests**
- **Jepsen tests** - Consistency and fault tolerance (runs on main branch)
- **Long-running load tests** - Ensuring that the system does not degrade with time

Verify all CI checks and the external test suites are passing before release.

### External Test Suites

#### Jepsen Tests

Repository: https://github.com/restatedev/jepsen

Jepsen tests validate Restate's consistency guarantees under various failure scenarios (network partitions, node failures, etc.).
The tests run against Restate's main branch.
Monitor the [Jepsen Actions](https://github.com/restatedev/jepsen/actions) page for results.

#### E2E Verification Runner

Repository: https://github.com/restatedev/e2e-verification-runner

The verification runner performs extended validation of Restate functionality. 
This includes backward and forward compatibility checks.
The tests run against `ghcr.io/restatedev/restate:main`.
Check the [Actions page](https://github.com/restatedev/e2e-verification-runner/actions) for test results.

> [!IMPORTANT]
> Make sure that the [`RESTATE_RELEASED_CONTAINER_IMAGE`](https://github.com/restatedev/e2e-verification-runner/blob/main/scripts/run-verification.sh#L8) is pointing to the latest released version of the previous minor release.

### Long-lived Load Tests

Follow the [instructions](https://github.com/restatedev/internal/blob/main/labs/lab-environment-long-lived-ec2/README.md) for how to update the Restate version for the long-lived load test.

- Update load test configurations for the new version
- Run load tests and verify performance metrics
- Compare results with previous release baselines

## Manual Testing Categories

The following categories might require manual testing before each minor release.

> [!IMPORTANT]
> Consider automating any manual test—it saves time on every future release. Add new e2e tests at https://github.com/restatedev/e2e.

### 1. Upgrade and Migration Testing

Test upgrading from the previous minor version (N-1) to the new release:

- Upgrade single-node deployments
- Upgrade cluster deployments
- Verify storage format compatibility
- Test that in-flight invocations complete correctly after upgrade

### 2. Rolling Upgrades (Mixed-Version Clusters)

Test rolling upgrade scenarios with mixed-version clusters:

- Start a cluster running the previous version (N-1)
- Upgrade nodes one at a time to the new version
- Verify cluster remains operational during the upgrade
- Verify requests are handled correctly with mixed versions
- Complete the upgrade and verify full functionality

### 3. Backward Compatibility Testing

Verify that older components work with the new server:

- Test older SDK versions against the new server
- Verify protocol version negotiation works correctly
- Ensure existing deployments continue to function
- [Test restoring a Cloud control plane snapshot](https://github.com/restatedev/restate-cloud/blob/main/scripts/restate-upgrade-test/README.md)

### 4. Rollback Testing

Verify that rolling back to the previous version (N-1) is possible:

- Start a cluster with the new version
- Put load on the cluster
- Roll back to the previous version
- Verify the cluster remains operational after rollback
- Verify no data corruption or loss occurred

### 5. Cluster Operations

Test cluster management operations:

- Expanding from single node to multi-node cluster
- Adding and removing nodes
- Failover and recovery scenarios
- Snapshot and restore operations

### 6. Feature-Specific Testing

Test new or significantly changed features for this release:

- UI functionality and new features
- Experimental features being promoted to stable
- New configuration options
- CLI/restatectl changes

### 7. Cloud Deployment Testing (if applicable)

If deploying to Restate Cloud:

- Test the release candidate in a cloud environment
- Verify UI functionality in cloud context
- Run load and introspection queries

### 8. Documentation Review

Before release, verify documentation accuracy:

- New features are documented
- Upgrade guides are accurate
- Configuration changes are documented
- Breaking changes are clearly communicated

### 9. Restate K8s Operator Testing

Make sure that the upcoming release works with Restate's [k8s operator](https://github.com/restatedev/restate-operator).

## Creating a Release Testing Issue

### Process

1. Create an umbrella issue titled "Testing Restate X.Y"
2. Link it to the release milestone
3. Create sub-issues for each testing category above
4. Assign owners to each sub-issue
5. Track progress and ensure all items are completed before release

### Issue Template

Use the following template for the umbrella issue:

```markdown
Umbrella issue for tracking release testing tasks for vX.Y.

**Important:** Only start testing tasks once the release has been frozen
(except for well-isolated features).

When testing new features, always verify that documentation is up-to-date.

Whenever possible, try to automate manual tests to save us time for future releases.

### Sub-issues

<!-- Create and link sub-issues for each category -->

- [ ] Upgrade and migration testing
- [ ] Rolling upgrades (mixed-version clusters)
- [ ] Backward compatibility testing
- [ ] Rollback testing
- [ ] Cluster operations testing
- [ ] Feature-specific testing (list features)
- [ ] Cloud deployment testing
- [ ] Documentation review
- [ ] Restate k8s operator testing
- [ ] Jepsen tests passing
- [ ] E2E verification runner passing
- [ ] Load tests completed
```

## See Also

- [Release Process](release.md) - How to perform the actual release
