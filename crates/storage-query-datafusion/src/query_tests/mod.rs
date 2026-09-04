// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! # Storage query test guide
//!
//! The tests in this module exercise DataFusion queries against Restate storage through the real
//! remote-scanner path. They do not register fabricated Arrow batches or execute SQL `INSERT`
//! statements. [`QueryTest::populate`](harness::QueryTest::populate) converts the inline text
//! tables into Restate storage types, writes them through the corresponding storage transaction,
//! and commits every affected partition before the query runs.
//!
//! ## Adding a query test
//!
//! Put the test next to other tests for the table it primarily exercises and register a new module
//! below only when a suitable module does not already exist. A test normally has three parts:
//!
//! 1. Create [`QueryTest`](harness::QueryTest) with `QueryTest::create_remote()`.
//! 2. Call `test.populate(...)` and populate one or more storage tables using inline text tables.
//! 3. Call `test.assert_query(...)` separately for every SQL query so the query, its expected
//!    result, and any failure are easy to identify. Use `test.assert_query_ordered(...)` when the
//!    query's `ORDER BY` is part of the behavior under test.
//!
//! ```ignore
//! #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
//! async fn query_example() {
//!     let mut test = QueryTest::create_remote().await;
//!     test.populate(|tables| {
//!         tables.state().populate_table(&[
//!             "+--------------+---------------------+---------+--------------+-------------+-------+-------+",
//!             "| partition_id | partition_key       | scope   | service_name | service_key | key   | value |",
//!             "+--------------+---------------------+---------+--------------+-------------+-------+-------+",
//!             "| 0            | 3169317165037139997 | scope-a | TestService  | key-1       | key-1 | one   |",
//!             "+--------------+---------------------+---------+--------------+-------------+-------+-------+",
//!         ])?;
//!         Ok(())
//!     })
//!     .await;
//!
//!     test.assert_query(QueryExpectation {
//!         name: "state keys",
//!         sql: "SELECT key FROM state WHERE service_name = 'TestService'",
//!         expected: &[
//!             "+-------+",
//!             "| key   |",
//!             "+-------+",
//!             "| key-1 |",
//!             "+-------+",
//!         ],
//!     })
//!     .await;
//! }
//! ```
//!
//! Keep both fixture data and expected results inline. The normal layout has table fields across
//! the header and one stored/result row per line. The parser also accepts the transposed
//! `column | row 1 | row 2` form when a value such as journal event JSON makes a horizontal table
//! materially harder to read. Empty cells represent absent optional values. A table adapter
//! validates its exact set of columns, so copy the header from a nearby test for that table and
//! change only the rows when possible.
//!
//! Populate every storage source used by a view. For example, `sys_invocation` joins durable
//! `sys_invocation_status` with live `sys_invocation_state`; use the same invocation ID in both
//! population tables when the result needs fields from both sides.
//!
//! ## `partition_id` and `partition_key`
//!
//! `partition_key` is the real Restate partition key. Depending on the table, it is either stored
//! in the identity or derived from an invocation ID, VQueue ID, service key, or scope. It is also
//! an SQL-visible internal column on many system tables.
//!
//! `partition_id` is different: it exists only in these fixture text tables. It is not written to
//! Restate storage and is not added to the DataFusion table schema. Its purpose is to make the
//! intended placement obvious to a reader. During population the harness independently resolves
//! the row's real partition key through the partition table and fails immediately if the resolved
//! partition does not equal the declared `partition_id`. Therefore changing only `partition_id`
//! never moves data; the row's real identity must have a partition key belonging to that partition.
//!
//! The remote test topology has one coordinator and three in-process scanner nodes. Partition 0 is
//! owned by node 2, partition 1 by node 3, and partition 2 by node 4. Each partition has its own
//! temporary [`PartitionStore`](restate_partition_store::PartitionStore), and its scanner node runs
//! the real remote query scanner server. This is not a full Restate deployment, but it covers query
//! planning, partition selection, remote scanner requests, and returned Arrow batches.
//!
//! Every `assert_query` clears the scan log, executes the SQL, compares the result table, and checks
//! that at least one remote scanner was opened and that each scanned partition was sent to its
//! configured owner. It intentionally does not require all three partitions: a point query that is
//! pruned to one partition is correct. To test fan-out, populate rows in partitions 0, 1, and 2 and
//! use a query whose predicates do not prune any of them.
//!
//! `assert_query` compares rows without considering their order, which avoids coupling unordered
//! distributed queries to scanner arrival order. `assert_query_ordered` compares the exact row
//! sequence. On failure both methods include the query, expected and actual tables, `EXPLAIN`, and
//! `EXPLAIN ANALYZE FORMAT TREE` output.
//!
//! Run all tests in this module with:
//!
//! ```text
//! cargo nextest run --all-features --no-capture \
//!   --package restate-storage-query-datafusion query_tests
//! ```

mod data;
mod deployments;
mod harness;
mod invocation_status;
mod journal;
mod locks;
mod state;
mod sys_invocation;
mod vqueue;
mod workflow;
