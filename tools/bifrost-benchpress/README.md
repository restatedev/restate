# Bifrost Bench Press

A simple tool for benchmarking bifrost loglet implementations

### How to run?
```sh
RUST_LOG=info cargo run --profile=bench --bin bifrost-benchpress -- --config-file=restate.toml --retain-test-dir write-to-read
```
