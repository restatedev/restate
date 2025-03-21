# Benchmarking Restate Server

- Launch a Restate server. You can build it from source with `cargo run --release --bin restate-server ...` or run a prebuilt binary using `docker run docker docker.io/restatedev/restate:latest --config-file .../restate.toml --base-dir .../restate-data`
- Start the mock Counter service available from the Restate source tree: `cargo run --release -p mock-service-endpoint --bin mock-service-endpoint`.
- Register the service: `restate deployments register http://localhost:9080 --yes`
- Run one of the provided wrk scripts, e.g. `run-test-400c-5m.sh`
