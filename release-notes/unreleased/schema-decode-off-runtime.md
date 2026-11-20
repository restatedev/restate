# Fewer leadership changes with large schemas

## Bug Fix

Nodes now load the schema, and free the previous one, on a separate thread pool. Before, loading a
large schema could pause a node long enough that it considered its peers dead and moved partition
leadership, which aborted in-flight invocations. No configuration changes are required.
