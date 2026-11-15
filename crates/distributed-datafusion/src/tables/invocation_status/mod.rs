// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::ops::{ControlFlow, RangeBounds};
use std::sync::Arc;

use bilrost::{Message, OwnedMessage};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};

use restate_partition_store::invocation_status_table::{
    InvocationStatusKey, InvocationStatusKeys, InvocationStatusRecords, InvocationStatusScan,
};
use restate_partition_store::keys::selection::KeySelection;
use restate_storage_query_api::selection::domain::Domain;
use restate_storage_query_api::tables::invocation_status::{
    InvocationStatus, InvocationStatusColumn,
};
use restate_storage_query_api::{LogicalSchema, RowSink, SelectionSchema};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_worker_api::PartitionCaptureRequirements;

use crate::access::{PrimaryKeys, PrimaryRead, PrimarySelection};
use crate::batches::{BatchSink, Batches, scan_rows_into};
use crate::context::SelectPartitions;
use crate::schema_scan::compile_selection;
use crate::sharding::PartitionSource;
use crate::source_registry::SourceRegistry;
use crate::statistics::{
    DEPLOYMENT_ROW_ESTIMATE, RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder,
};
use crate::table_providers::{
    PartitionReadContext, PartitionedTableProvider, PreparedPartitionRead, ReadMetrics,
    ScanPartition,
};

mod row;
#[cfg(test)]
mod tests;

crate::tables::table_adapter!(pub(crate) SysInvocationStatusTable, InvocationStatus);

impl PartitionSource for SysInvocationStatusTable {
    const ORDERING: &'static [InvocationStatusColumn] = &[InvocationStatusColumn::PartitionKey];
    const PRIMARY_ACCESS: Option<crate::access::SelectPrimary> = Some(select_primary);
}

impl SysInvocationStatusTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<InvocationStatus>::new()
            .with_num_rows_estimate(RowEstimate::Large)
            .with_partition_key(InvocationStatusColumn::PartitionKey)
            .with_primary_key(InvocationStatusColumn::Id)
            .with_foreign_key(
                InvocationStatusColumn::PinnedDeploymentId,
                DEPLOYMENT_ROW_ESTIMATE,
            )
            .with_foreign_key(
                InvocationStatusColumn::TargetServiceName,
                SERVICE_ROW_ESTIMATE,
            );
        Arc::new(
            PartitionedTableProvider::declared::<Self>(partitions, sources)
                .with_statistics(statistics.build()),
        )
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        StatusScanner
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, bilrost::Message)]
struct InvocationKey {
    #[bilrost(1)]
    partition_key: PartitionKey,
    #[bilrost(tag(2), encoding(plainbytes))]
    uuid: [u8; 16],
}

#[derive(bilrost::Message)]
struct InvocationKeys {
    #[bilrost(1)]
    keys: Vec<InvocationKey>,
}

#[derive(Debug)]
struct InvocationSelection(Vec<InvocationId>);

fn select_primary(
    filters: &[Arc<dyn PhysicalExpr>],
) -> anyhow::Result<Option<Box<dyn PrimarySelection>>> {
    let keys = InvocationStatus::analyze(filters, NonZeroUsize::new(16).unwrap())?
        .into_iter()
        .map(|domains| domains.id)
        .fold(Domain::empty(), Domain::union)
        .into_values();
    Ok(keys.map(|keys| Box::new(InvocationSelection(keys)) as Box<dyn PrimarySelection>))
}

impl PrimarySelection for InvocationSelection {
    fn within(&self, ownership: KeyRange) -> Option<PrimaryKeys> {
        let first = self
            .0
            .partition_point(|id| id.partition_key() < ownership.start());
        let end = self
            .0
            .partition_point(|id| id.partition_key() <= ownership.end());
        (first < end).then(|| PrimaryKeys {
            count: (end - first) as u64,
            encoded: InvocationKeys {
                keys: self.0[first..end]
                    .iter()
                    .map(|id| InvocationKey {
                        partition_key: id.partition_key(),
                        uuid: id.invocation_uuid().to_bytes(),
                    })
                    .collect(),
            }
            .encode_to_vec(),
        })
    }
}

#[derive(Debug)]
struct StatusScanner;

impl ScanPartition for StatusScanner {
    fn supports_primary_lookup(&self) -> bool {
        true
    }

    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        let access = match access {
            PrimaryRead::Scan => StatusAccess::Scan(Arc::new(InvocationStatusKeys::prepare(
                &compile_selection::<InvocationStatus>(predicate.as_ref())?,
                crate::selection::key_range(&InvocationStatus::candidate_shard_keys(
                    predicate.as_slice(),
                )?),
            )?)),
            PrimaryRead::Index(index) => anyhow::bail!("unsupported index '{index}'"),
            PrimaryRead::MultiGet(keys) => {
                keys.validate()?;
                let decoded = InvocationKeys::decode(keys.encoded.as_slice())?;
                anyhow::ensure!(
                    decoded.keys.len() == keys.len()
                        && decoded.keys.windows(2).all(|pair| pair[0] < pair[1]),
                    "invalid invocation primary key set"
                );
                StatusAccess::Keys(
                    decoded
                        .keys
                        .into_iter()
                        .map(|key| {
                            InvocationId::from_parts(
                                key.partition_key,
                                InvocationUuid::from_bytes(key.uuid),
                            )
                        })
                        .collect(),
                )
            }
        };
        Ok(Arc::new(StatusRead {
            access,
            projection,
            limit,
        }))
    }
}

#[derive(derive_more::Debug, Clone)]
enum StatusAccess {
    Scan(#[debug(skip)] Arc<KeySelection<InvocationStatusKey>>),
    Keys(Arc<[InvocationId]>),
}

#[derive(Debug)]
struct StatusRead {
    access: StatusAccess,
    projection: SchemaRef,
    limit: Option<usize>,
}

impl PreparedPartitionRead for StatusRead {
    fn capture_requirements(&self) -> PartitionCaptureRequirements {
        PartitionCaptureRequirements {
            storage: true,
            ..Default::default()
        }
    }

    fn execute(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        anyhow::ensure!(batch_size > 0, "batch size must be positive");
        let (store, executor) = context.storage_read()?;
        let (store, executor) = (store.clone(), executor.clone());
        let projection = Arc::clone(&self.projection);
        let access = self.access.clone();
        let limit = self.limit;
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&projection), 1);
        let tx = stream.tx();
        stream.spawn(async move {
            if limit == Some(0) {
                return Ok(());
            }
            let filter = match access {
                // The cursor intersects the selection with the snapshot's ownership.
                StatusAccess::Scan(request) => InvocationStatusScan::Keys(request),
                StatusAccess::Keys(keys) => {
                    let ownership = store.partition_key_range();
                    if keys
                        .iter()
                        .any(|id| !ownership.contains(&id.partition_key()))
                    {
                        return datafusion::common::exec_err!(
                            "primary keys are outside their partition"
                        );
                    }
                    InvocationStatusScan::Ids(keys.iter().copied().collect())
                }
            };
            scan_rows_into::<InvocationStatusRecords, InvocationStatus, _>(
                store,
                &executor,
                Arc::new(filter),
                BatchSink::new(
                    Batches::<InvocationStatus>::new(
                        projection,
                        batch_size,
                        limit,
                        metrics.elapsed_compute,
                    ),
                    tx.clone(),
                ),
                |(id, status), batches: &mut Batches<InvocationStatus>| {
                    if let ControlFlow::Break(result) = batches.append_with(|builder| {
                        row::append_row(builder, id, &status).map_err(|error| {
                            restate_storage_api::StorageError::Conversion(error.into())
                        })
                    }) {
                        result?;
                    }
                    Ok(())
                },
            )
            .await
        });
        Ok(stream.build())
    }
}
