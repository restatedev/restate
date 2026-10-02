# Release Testing Guide

Use this checklist to prepare testing for a major/minor release. Tailor the tasks to the release's changes and reuse existing automation.

## Getting Started

- Prepare tasks before the release freeze; run final validation on the frozen candidate. Well-isolated features can be tested earlier.
- Test the prepared release commit or candidate image, following the [release process](release.md).
- Check the release notes for supported upgrade/rollback versions, required configuration, and one-way feature changes.

## Automated Suites

| Suite | What to check |
| --- | --- |
| [CI](../../.github/workflows/ci.yml) | Build, unit/integration tests, SDK integration tests, and E2E tests pass for the candidate. Review relevant non-blocking failures and exclusions too. |
| [Jepsen](https://github.com/restatedev/jepsen/actions) | Consistency and fault-tolerance tests pass. Runs are dispatched [nightly](../../.github/workflows/jepsen-nightly-dispatch.yml); use the [workflow inputs](https://github.com/restatedev/jepsen/blob/main/.github/workflows/jepsen.yml) to select the candidate for release testing. |
| [E2E verification runner](https://github.com/restatedev/e2e-verification-runner/actions) | Correctness and upgrade/downgrade tests pass with the intended candidate, baseline versions, and configuration. |
| [Long-lived load tests](https://github.com/restatedev/internal/blob/main/labs/lab-environment-long-lived-ec2/README.md) | Update the environment to the candidate, check for degradation over time, and compare performance and resource use with the previous release. |

For compatibility tests, use the latest supported patch of the previous minor release and any additional versions needed to exercise compatibility boundaries. Check both `RESTATE_RELEASED_CONTAINER_IMAGE` in the [runner script](https://github.com/restatedev/e2e-verification-runner/blob/main/scripts/run-verification.sh) and overrides in the [workflow matrices](https://github.com/restatedev/e2e-verification-runner/tree/main/.github/workflows).

## Testing Checklist

Use automated results where they cover the scenario; test the remaining gaps. Extend [E2E tests](https://github.com/restatedev/e2e) or [local cluster tests](../../server/tests) where practical, and track release-specific automation work in the testing issue.

### Upgrade, Mixed Versions, and Rollback

- Upgrade single-node and cluster deployments from supported versions with persisted data and in-flight invocations.
- Roll cluster nodes one at a time under load; check progress and correct results during mixed-version operation and after upgrade.
- Where supported, roll back under load and re-upgrade. Verify persisted state and invocation results, using the required rollback configuration and respecting one-way feature changes.

### SDK and Deployment Compatibility

- Check older supported SDKs and existing service deployments against the candidate, including protocol negotiation and resuming in-flight work.
- Check documented errors and migration guidance for compatibility that the release removes.

### Cluster Operations

- Exercise single-node expansion, adding/removing nodes, and failover/recovery under load. Check progress and correct results after each operation.
- Exercise snapshot and restore, verifying restored state and resumed work.

### Features and Documentation

- Test new or changed features, promoted experimental features, configuration, UI, and CLI/restatectl workflows.
- Follow the documentation while testing; fix missing or confusing steps. Check upgrade guidance, configuration changes, and breaking-change notices.

### Cloud and Kubernetes

- In Restate Cloud, test the candidate with the UI, load, and introspection queries, and [restore a Cloud control plane snapshot](https://github.com/restatedev/restate-cloud/blob/main/scripts/restate-upgrade-test/README.md).
- With the [Kubernetes operator](https://github.com/restatedev/restate-operator), check deployment, upgrade, scaling, and recovery.
- Check existing probes, dashboards, and operational queries after upgrade.

### Time-boxed Claude/Codex Exploration

- Give Claude/Codex a time budget to deploy a disposable cluster, run workloads with checkable results, and try to break Restate through cluster operations and injected failures.
- Check correctness and recovery; judge availability against the deployment's configured fault tolerance.

## Tracking the Release Tests

1. Create an umbrella issue titled **Testing Restate X.Y** and link it to the release milestone.
2. Use the suites and checklist above to create tasks, adding release-specific feature and automation tasks. Assign an owner to each; group related checks where useful.
3. Report the scenario, environment/version, and outcome in a short comment. An automated run link is enough if it contains that information. Link bugs and include reproduction details for failures.
4. Before release, check that applicable tests passed and release-blocking findings are resolved. Note any skipped checks or deferred automation in the issue.

### Umbrella Issue Template

```markdown
Testing Restate vX.Y, following https://github.com/restatedev/restate/blob/main/docs/dev/release-testing.md.

### Tasks

<!-- Link owned tasks for the applicable suites, checklist items, and release-specific work. -->
- [ ] <task link> — <owner>
```
