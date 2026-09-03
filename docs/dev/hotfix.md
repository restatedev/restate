# Building a Hotfix Image

Prefer a regular patch release for fixes that can follow the normal release process. Use a hotfix image when a customer needs a fix before that release is available.

A hotfix is a GHCR image, not a full Restate release. It does not update Docker Hub, `latest`, minor-version tags, binaries, npm, Homebrew, or the Helm chart registry.

## Naming

For a hotfix based on `v1.7.4`, use:

| Item | Name |
| --- | --- |
| Branch | `hotfix/v1.7.4+hotfix.1` |
| Cargo and Helm version | `1.7.4+hotfix.1` |
| Git tag | `v1.7.4+hotfix.1` |
| Image tag | `ghcr.io/restatedev/restate:1.7.4-hotfix.1` |

Increment `N` for each hotfix built from the same release. Hotfix numbers identify independent builds; they do not imply that the fixes are cumulative.

## Build

1. Choose the exact release tag to patch and the next unused hotfix number.
2. Create `hotfix/vX.Y.Z+hotfix.N` from `vX.Y.Z`.
3. Apply the smallest required fix. Prefer a fix that is already merged, or intended to be merged, into a maintained branch.
4. Set the workspace version in `Cargo.toml` and the chart version in `charts/restate-helm/Chart.yaml` to `X.Y.Z+hotfix.N` and update `Cargo.lock`.
5. Run CI and any tests relevant to the fix.
6. Create and push the tag `vX.Y.Z+hotfix.N` from the tested commit.
7. Run the `Build Docker image` workflow from the default branch. Set `hotfixTag` to `vX.Y.Z+hotfix.N`, keep `profile` set to `release`, select the required runner size, and leave `pushToDockerHub` disabled.
8. Verify that the image reports `X.Y.Z+hotfix.N` and record the digest from the workflow summary.
9. Give the customer the image tag and digest. The digest is the immutable reference to the delivered image.
10. Delete the hotfix branch when it is no longer needed, but retain the Git tag and image.
11. Include the fix in the next regular patch release and move the customer to that release when available.

The workflow refuses to overwrite an existing hotfix image tag. If a published image needs to change, increment `N` and build a new hotfix.
