# Distributed DataFusion prototype

Experimental query-engine crate copied from `restate-storage-query-datafusion` at
`08e715ceb960` (`[Datafusion][6/N]`). It shares `restate-storage-query-api` with the
existing engine. The initial source copy retains the baseline scanner execution
path; distributed stage/task execution is the next milestone.

Run the crate's baseline tests with:

```sh
cargo nextest run -p restate-distributed-datafusion --all-features
```
