// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection table `sys_proto_schemas` listing all dynamically registered Protobuf
//! state schemas across services.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use restate_types::schema::service::ServiceMetadataResolver;

use super::DynamicProtoSchemaRegistry;
use crate::context::QueryContext;
use crate::statistics::{SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_macro::*;
use crate::table_providers::{GenericTableProvider, Scan};
use crate::table_util::Builder;

define_table!(
    /// Dynamically registered Protobuf schemas for Virtual Object and Workflow state.
    ///
    /// Populated automatically from service discovery metadata (`proto.descriptor_set`
    /// and `proto.state.<key>`).
    sys_proto_schemas(
    /// The name of the registered user service.
    service_name: DataType::LargeUtf8,

    /// The revision of the service that registered this schema.
    revision: DataType::UInt64,

    /// The state key mapped to this Protobuf message (or `*` for wildcard default).
    state_key: DataType::LargeUtf8,

    /// Fully qualified Protobuf message name (e.g. `previ.bench.BusyMemberProfile`).
    message_name: DataType::LargeUtf8,

    /// JSON array describing all fields (`name`, `number`, `type`, `repeated`, `map`) of the message.
    fields_json: DataType::LargeUtf8,
));

pub(crate) fn register_self<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    ctx: &QueryContext,
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> datafusion::common::Result<()> {
    let schema = SysProtoSchemasBuilder::schema();
    let statistics =
        TableStatisticsBuilder::new(schema).with_num_rows_estimate(SERVICE_ROW_ESTIMATE);
    let table = GenericTableProvider::new(
        SysProtoSchemasBuilder::schema(),
        Arc::new(ProtoSchemasScanner(registry)),
    )
    .with_statistics(statistics.build());

    ctx.register_non_partitioned_table("sys_proto_schemas", Arc::new(table))
}

#[derive(Clone, derive_more::Debug)]
#[debug("ProtoSchemasScanner")]
struct ProtoSchemasScanner<SMR>(DynamicProtoSchemaRegistry<SMR>);

#[inline]
fn append_proto_schema_row(
    builder: &mut SysProtoSchemasBuilder,
    service_name: &str,
    revision: u32,
    state_key: &str,
    msg: &super::CompiledMessageSchema,
) {
    let mut row = builder.row();
    row.service_name(service_name);
    row.revision(u64::from(revision));
    row.state_key(state_key);
    row.message_name(&msg.full_name);
    if row.is_fields_json_defined() {
        row.fields_json(msg.fields_summary_json());
    }
}

impl<SMR: ServiceMetadataResolver + Sync + Send + 'static> Scan for ProtoSchemasScanner<SMR> {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        _limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();
        let services = self.0.list_all_service_schemas();

        stream_builder.spawn(async move {
            let mut builder = SysProtoSchemasBuilder::new(schema);
            for svc in services {
                let mut keys: Vec<(&str, &Arc<super::CompiledMessageSchema>)> = svc
                    .state_key_messages
                    .iter()
                    .map(|(k, v)| (k.as_str(), v))
                    .collect();
                keys.sort_by_key(|(k, _)| *k);
                if let Some(wild) = &svc.wildcard_message {
                    keys.push(("*", wild));
                }
                for (state_key, msg) in keys {
                    append_proto_schema_row(
                        &mut builder,
                        &svc.service_name,
                        svc.revision,
                        state_key,
                        msg,
                    );
                    if builder.num_rows() >= batch_size {
                        let batch: datafusion::common::Result<RecordBatch> =
                            builder.finish_and_new();
                        if tx.send(batch).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
            if !builder.empty() {
                let _ = tx.send(builder.finish()).await;
            }
            Ok(())
        });
        stream_builder.build()
    }
}
