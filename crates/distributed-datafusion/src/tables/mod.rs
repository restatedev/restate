// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! V2 table providers and source adapters for shared logical declarations.

/// Bridges a shared logical declaration to the V2 catalog.
macro_rules! table_adapter {
    ($vis:vis $table:ident, $builder:ident, $schema:ty) => {
        crate::tables::table_adapter!($vis $table, $schema);
        $vis type $builder = <$schema as restate_storage_query_api::LogicalSchema>::RowBuilder;
    };
    ($vis:vis $table:ident, $schema:ty) => {
        #[derive(Debug, Clone)]
        $vis struct $table;

        impl crate::QuerySource for $table {
            type Schema = $schema;
        }
    };
}

pub(crate) use table_adapter;

pub mod bifrost_read_stream;
pub(crate) mod deployment;
pub(crate) mod index;
pub(crate) mod invocation_state;
pub(crate) mod invocation_status;
pub(crate) mod journal;
pub(crate) mod journal_events;
pub(crate) mod locks;
pub mod loglet_workers;
pub(crate) mod logs;
pub(crate) mod migration_records;
pub(crate) mod nodes;
pub(crate) mod partition_replica_set;
pub(crate) mod partition_state;
pub(crate) mod partitions;
pub(crate) mod promise;
pub(crate) mod rules;
pub(crate) mod scheduler_status;
pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod stats;
pub(crate) mod user_limits;
pub(crate) mod vqueue_entry_status;
pub(crate) mod vqueue_meta;
