// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod access;
mod batches;
pub mod context;
mod diagnostics;
mod environment;
mod explain;
mod live_scanners;
pub mod local_scanners;
pub use catalog::{
    ClusterTables, MetadataTables, RegisterTable, TableInventoryBuilder, UserTables,
};
pub use environment::DataFusionEnv;
pub use sharding::QuerySource;

mod catalog;
pub mod node_fan_out;
pub mod placement;
mod schema_scan;
mod selection;
mod sharding;
mod statistics;
#[cfg(feature = "table_docs")]
pub mod table_docs;
mod table_providers;
pub mod tables;
pub use table_providers::Scan;
mod table_util;

pub use context::BuildError;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::convert::IpcSchemaEncoder;
use datafusion::arrow::ipc::writer::DictionaryTracker;

#[cfg(test)]
pub(crate) mod mocks;
#[cfg(test)]
mod query_correctness;

pub mod distributed;
pub mod source_registry;
#[cfg(test)]
mod tests;

pub(crate) fn encode_schema(schema: &Schema) -> Vec<u8> {
    let mut dictionary_tracker = DictionaryTracker::new(true);
    let fb = IpcSchemaEncoder::new()
        .with_dictionary_tracker(&mut dictionary_tracker)
        .schema_to_fb(schema);
    let ipc_bytes = fb.finished_data();
    ipc_bytes.to_vec()
}

pub(crate) fn decode_schema(ipc_bytes: &[u8]) -> anyhow::Result<Schema> {
    let ipc_schema = datafusion::arrow::ipc::root_as_schema(ipc_bytes)
        .map_err(|e| anyhow::anyhow!("unable to decode {}", e))?;
    let schema = datafusion::arrow::ipc::convert::fb_to_schema(ipc_schema);
    Ok(schema)
}

#[cfg(test)]
mod serde_tests {
    #[test]
    pub fn round_trip_schema() {
        let schema =
            crate::tables::vqueue_entry_status::schema::SysVqueueEntryStatusBuilder::schema();
        let buf = super::encode_schema(&schema);

        let got = super::decode_schema(&buf).expect("deserialization works");

        assert_eq!(*schema, got);
    }
}
