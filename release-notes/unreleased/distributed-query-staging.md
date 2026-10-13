# Experimental query-engine comparisons

Staging clusters can opt into an experimental distributed query engine by setting
`experimental-enable-query-engine-v2 = true` on participating nodes and restarting
them. Send `X-Restate-Query-Engine: v2` to the admin `/query` endpoint to
try it, or `v1` to compare against the existing engine. To make the distributed
engine the default for requests without this header, also set
`experimental-enable-query-engine-v2-default = true` on admin nodes and restart
them. Explicit headers override the default. Otherwise, headerless requests use
the existing engine.

Upgrade all participating nodes to this version before enabling the distributed
engine. Use the existing engine during rolling upgrades; distributed queries
reject peers that do not support the new query protocol.

Query responses identify the selected engine and include planning duration in the
`Server-Timing` header. Use `EXPLAIN VERBOSE` and `EXPLAIN ANALYZE VERBOSE` to compare
plans and execution. The experimental engine supports queries spanning multiple
storage owners and prunes node queries by plain or exact generational node identity.
Eligible filters, aggregations, and sorts run at storage owners. Query parallelism
now adapts to the selected storage work, up to the configured target, avoiding
unnecessary fan-out for small or selective queries. Full-table counts and grouped
summaries reduce data transfer, and sorting work is shared across storage owners.

Fixed queries returning more rows than requested by `LIMIT` without `ORDER BY`
in the experimental engine, including limits inside subqueries.

Detailed remote operator metrics are not yet available. Upgrade all participating
prototype nodes together when trying this version.
