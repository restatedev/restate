# Releasing the Restate runtime

In order to release the Restate runtime, you have to create a tag of the form `vX.Y.Z` and push it to the repository.
The tag will trigger the [release.yml](/.github/workflows/release.yml) workflow which does the following:

* Running the local tests
* Creating and pushing the Docker image with the runtime
* Creating the CLI binaries
* Creating a draft release with the CLI binaries attached

In order to finish the release, you have to publish it [here](https://github.com/restatedev/restate/releases).