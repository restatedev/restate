# Readable EXPLAIN output for large literal sets

## Behavioral Change

### What Changed

The V2 query engine's default `EXPLAIN` output summarizes large literal `IN` and
`NOT IN` lists in both logical and physical plans. Each summary includes the
number of values, distinct count, a short sample, and a set identifier shared
across that explanation. Surrounding conditions remain visible, including
filters after a long list and predicates in distributed stages.

### Impact on Users

Use `EXPLAIN VERBOSE` when you need the complete literal values and optimizer
steps. Small lists retain their existing presentation. Query results and
execution behavior are unchanged. `EXPLAIN ANALYZE` and alternative EXPLAIN
formats retain their existing presentation.
