// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_storage_query_api::LogicalSchema;
use restate_storage_query_api::ordering::{NativeOrder, OrderFunction, OrderInput, OrderTerm};
use restate_storage_query_api::tables::invocation_state::{InvocationState, InvocationStateColumn};
use restate_types::identifiers::WithPartitionKey;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::time::MillisSinceEpoch;
use restate_worker_api::PartitionCaptureRequirements;
use restate_worker_api::invoker::InvocationStatusReport;

use crate::context::SelectPartitions;
use crate::live_scanners::LivePartitionScanner;
use crate::source_registry::SourceRegistry;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

crate::tables::table_adapter!(pub(crate) SysInvocationStateTable, SysInvocationStateBuilder, InvocationState);

impl crate::sharding::PartitionSource for SysInvocationStateTable {
    /// The invoker reports its in-flight invocations sorted by ID
    /// (`ChannelStatusReader::try_read_status`), and a read keeps that order; its
    /// scanner fails a read that doesn't come in it.
    const ORDERS: &'static [NativeOrder] = &[
        &[OrderTerm::Column("partition_key")],
        &[OrderTerm::Function(OrderFunction::InvocationId)],
    ];
    const ID_COLUMN: Option<InvocationStateColumn> = Some(InvocationStateColumn::Id);
}

impl SysInvocationStateTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<InvocationState>::new()
            .with_num_rows_estimate(RowEstimate::Small)
            .with_partition_key(InvocationStateColumn::PartitionKey)
            .with_primary_key(InvocationStateColumn::Id);
        Arc::new(
            PartitionedTableProvider::declared::<Self>(partitions, sources)
                .with_statistics(statistics.build()),
        )
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        LivePartitionScanner::<InvocationState, _>::new(
            PartitionCaptureRequirements {
                invoker: true,
                ..Default::default()
            },
            InvocationState::candidate_shard_keys,
            |captured| captured.invoker.as_ref(),
            |report| report.invocation_id().partition_key(),
            append_row,
        )
        .ordered_by(
            |report, function, with| {
                report
                    .invocation_id()
                    .order_key(function, |bytes| with(bytes))
                    .is_some()
            },
            |previous, report| previous.invocation_id() <= report.invocation_id(),
        )
    }
}

fn append_row(builder: &mut SysInvocationStateBuilder, report: &InvocationStatusReport) {
    let mut row = builder.row();
    row.partition_key(report.invocation_id().partition_key());
    if row.is_id_defined() {
        row.fmt_id(report.invocation_id());
    }
    row.in_flight(report.in_flight());
    row.retry_count(report.retry_count() as u64);
    row.last_start_at(MillisSinceEpoch::as_u64(&report.last_start_at().into()) as i64);
    if row.is_last_attempt_deployment_id_defined()
        && let Some(id) = report.last_attempt_deployment_id()
    {
        row.last_attempt_deployment_id(id.to_string());
    }
    if let Some(server) = report.last_attempt_server() {
        row.last_attempt_server(server);
    }
    if let Some(at) = report.next_retry_at() {
        row.next_retry_at(MillisSinceEpoch::as_u64(&at.into()) as i64);
    }
    if let Some(failure) = report.last_retry_attempt_failure() {
        if row.is_last_failure_defined() {
            row.fmt_last_failure(&failure.err);
        }
        if let Some(code) = failure.doc_error_code {
            row.last_failure_error_code(code.code());
        }
        match report.last_attempt_service_protocol_version() {
            None => {}
            Some(version) if *version <= ServiceProtocolVersion::V3 => {
                if let Some(name) = &failure.related_entry_name
                    && !name.is_empty()
                {
                    row.last_failure_related_entry_name(name);
                }
                if let Some(index) = failure.related_entry_index {
                    row.last_failure_related_entry_index(index as u64);
                }
                if row.is_last_failure_related_entry_type_defined()
                    && let Some(ty) = &failure.related_entry_type
                {
                    row.fmt_last_failure_related_entry_type(ty);
                }
            }
            _ => {
                if let Some(name) = &failure.related_entry_name
                    && !name.is_empty()
                {
                    row.last_failure_related_command_name(name);
                }
                if let Some(index) = failure.related_entry_index {
                    row.last_failure_related_command_index(index as u64);
                }
                if row.is_last_failure_related_command_type_defined()
                    && let Some(ty) = &failure.related_entry_type
                {
                    row.fmt_last_failure_related_command_type(ty);
                }
            }
        }
    }
    if row.is_last_awaiting_on_future_json_defined()
        && let Some(future) = report.last_awaiting_on_unresolved_future()
    {
        match serde_json::to_string(future) {
            Ok(json) => {
                row.last_awaiting_on_future_json(json);
            }
            Err(error) => {
                tracing::error!(%error, invocation_id = %report.invocation_id(), "Cannot serialize invocation future")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::array_value_to_string;
    use futures::TryStreamExt;

    use restate_types::errors::InvocationError;
    use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionId};
    use restate_worker_api::invoker::InvocationErrorReport;
    use restate_worker_api::invoker::status_handle::InvocationStatusReportInner;
    use restate_worker_api::{CapturedPartitionRead, CapturedPartitionState};

    use crate::access::PrimaryRead;
    use crate::table_providers::{PartitionReadContext, ReadMetrics};

    use super::*;

    /// Captured rows come in ID order, each with its key field; rows that don't
    /// fail the read instead of passing for ordered.
    #[restate_core::test]
    async fn captured_invoker_rows_carry_key_fields_in_id_order() {
        use datafusion::arrow::array::{Array, FixedSizeBinaryArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let ids = [(7, 1), (7, 256), (8, 0)]
            .map(|(key, uuid)| InvocationId::from_parts(key, InvocationUuid::from_u128(uuid)));
        let function = OrderFunction::InvocationId;
        let field = function.key_field("id");
        let schema = Arc::new(Schema::new(
            InvocationState::arrow_schema()
                .fields()
                .iter()
                .cloned()
                .chain([Arc::new(Field::new(
                    &field,
                    DataType::FixedSizeBinary(function.width() as i32),
                    false,
                ))])
                .collect::<Vec<_>>(),
        ));
        let scanner = Arc::new(SysInvocationStateTable::create_local_scanner());
        let read = async |ids: &[InvocationId]| {
            let reports: Vec<_> = ids
                .iter()
                .map(|id| InvocationStatusReport::new(*id, InvocationStatusReportInner::default()))
                .collect();
            let context = PartitionReadContext::from_capture(CapturedPartitionRead {
                partition: PartitionId::MIN,
                storage: None,
                live: CapturedPartitionState {
                    invoker: Some(reports.into()),
                    ..Default::default()
                },
                timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
            });
            Arc::clone(&scanner)
                .prepare_read(PrimaryRead::Scan, Arc::clone(&schema), None, None)
                .unwrap()
                .execute(context, 8, ReadMetrics::default())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
        };
        let batches = read(&ids).await.unwrap();
        let keys = batches[0]
            .column_by_name(&field)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(keys.len(), ids.len());
        for (at, id) in ids.iter().enumerate() {
            assert_eq!(keys.value(at), id.to_bytes());
        }
        let mut reversed = ids;
        reversed.reverse();
        let error = read(&reversed).await.unwrap_err();
        assert!(
            error.to_string().contains("came out of the order"),
            "{error}"
        );
    }

    #[restate_core::test]
    async fn captured_invoker_rows_preserve_protocol_columns_and_projection() {
        let id = InvocationId::from_parts(7, InvocationUuid::from_u128(1));
        let reports: Vec<_> = [ServiceProtocolVersion::V3, ServiceProtocolVersion::V4]
            .into_iter()
            .map(|version| {
                InvocationStatusReport::new(
                    id,
                    InvocationStatusReportInner {
                        in_flight: true,
                        start_count: 3,
                        last_attempt_protocol_version: Some(version),
                        last_attempt_server: Some("test-sdk".into()),
                        last_retry_attempt_failure: Some(InvocationErrorReport {
                            err: InvocationError::internal("retry"),
                            doc_error_code: None,
                            related_entry_index: Some(4),
                            related_entry_name: Some("command".into()),
                            related_entry_type: None,
                        }),
                        ..Default::default()
                    },
                )
            })
            .collect();
        let scanner = Arc::new(SysInvocationStateTable::create_local_scanner());
        let prepared = scanner
            .clone()
            .prepare_read(
                PrimaryRead::Scan,
                InvocationState::arrow_schema(),
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            prepared.capture_requirements(),
            PartitionCaptureRequirements {
                invoker: true,
                ..Default::default()
            }
        );
        let context = PartitionReadContext::from_capture(CapturedPartitionRead {
            partition: PartitionId::MIN,
            storage: None,
            live: CapturedPartitionState {
                invoker: Some(reports.into()),
                ..Default::default()
            },
            timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
        });
        let batches = prepared
            .execute(context.clone(), 1, ReadMetrics::default())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 2);
        for (index, batch) in batches.iter().enumerate() {
            let value =
                |name| array_value_to_string(batch.column_by_name(name).unwrap(), 0).unwrap();
            assert_eq!(value("id"), id.to_string());
            assert_eq!(value("retry_count"), "3");
            assert_eq!(value("in_flight"), "true");
            assert_eq!(value("last_attempt_server"), "test-sdk");
            let (present, absent) = if index == 0 {
                (
                    "last_failure_related_entry_index",
                    "last_failure_related_command_index",
                )
            } else {
                (
                    "last_failure_related_command_index",
                    "last_failure_related_entry_index",
                )
            };
            assert_eq!(value(present), "4");
            assert_eq!(batch.column_by_name(absent).unwrap().null_count(), 1);
        }
        let projected = scanner
            .prepare_read(
                PrimaryRead::Scan,
                Arc::new(InvocationState::arrow_schema().project(&[1, 3]).unwrap()),
                None,
                Some(1),
            )
            .unwrap();
        let batches = projected
            .execute(context, 8, ReadMetrics::default())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
        // Empty projection must still count rows.
        let mut builder =
            SysInvocationStateBuilder::new(Arc::new(datafusion::arrow::datatypes::Schema::empty()));
        append_row(
            &mut builder,
            &InvocationStatusReport::new(id, Default::default()),
        );
        assert_eq!(builder.into_batch().unwrap().num_rows(), 1);
    }
}
