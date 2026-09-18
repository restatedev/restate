# Upgrade the SQL query engine to DataFusion 55

## Behavioral Change

Restate's SQL query engine now uses DataFusion 55.1.0, Arrow 59.3.0, and the
matching DataFusion JSON functions and protobuf support. Query planning and SQL
behavior follow the new upstream release; see the
[DataFusion 55 upgrade guide](https://github.com/apache/datafusion/blob/55.1.0/docs/source/library-user-guide/upgrading/55.0.0.md)
for details.

Restate's custom scans now expose their physical predicates through DataFusion's
expression traversal API. Restate configuration keys and introspection table
definitions are unchanged.

DataFusion now enables Serde JSON's `preserve_order` feature. JSON object keys may
therefore be emitted in insertion order rather than sorted order. Consumers
should compare JSON objects structurally rather than relying on key order.
