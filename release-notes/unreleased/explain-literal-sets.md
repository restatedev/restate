# Readable EXPLAIN output for large literal sets

## Behavioral Change

### What Changed

The V2 query engine's default indented `EXPLAIN` and `EXPLAIN ANALYZE` output
summarizes large literal `IN` and `NOT IN` lists throughout the plans. Each summary
includes the number of values, distinct count, a short sample, and a set identifier
shared across that explanation. Surrounding conditions remain visible, including
filters after a long list and predicates in distributed stages.

### Impact on Users

`ANALYZE` executes the query and adds runtime metrics; it does not select the
literal detail level. Use `EXPLAIN VERBOSE` or `EXPLAIN ANALYZE VERBOSE` for
DataFusion's native detailed output, including complete literal values.
Small lists retain their existing presentation. Query results, execution
behavior, and metric selection are unchanged. Alternative EXPLAIN formats
retain their existing presentation.
