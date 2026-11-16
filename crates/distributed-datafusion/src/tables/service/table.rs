// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::sync::mpsc::Sender;

use restate_storage_query_api::tables::service::{Service, ServiceColumn};
use restate_types::live::Live;
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};

use super::row::append_service_row;
use super::schema::{SysServiceBuilder, SysServiceTable};
use crate::source_registry::SourceRegistry;
use crate::statistics::{SERVICE_ROW_ESTIMATE, TableStatisticsBuilder};
use crate::table_providers::{GenericTableProvider, Scan};

impl SysServiceTable {
    pub(crate) fn create_provider(sources: SourceRegistry) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<Service>::new()
            .with_num_rows_estimate(SERVICE_ROW_ESTIMATE)
            .with_primary_key(ServiceColumn::Name);
        Arc::new(
            GenericTableProvider::declared::<Self>(None, Some(sources))
                .with_statistics(statistics.build()),
        )
    }

    pub(crate) fn create_local_scanner(
        resolver: Live<impl ServiceMetadataResolver + Send + Sync + 'static>,
    ) -> impl Scan {
        ServiceMetadataScanner(resolver)
    }
}

#[derive(Clone, derive_more::Debug)]
#[debug("ServiceMetadataScanner")]
struct ServiceMetadataScanner<SMR>(Live<SMR>);

impl<SMR: ServiceMetadataResolver + Sync + Send + 'static> Scan for ServiceMetadataScanner<SMR> {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();

        let rows = self.0.pinned().list_services();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, rows, batch_size, limit).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: Vec<ServiceMetadata>,
    batch_size: usize,
    limit: Option<usize>,
) {
    let mut builder = SysServiceBuilder::new(schema.clone());
    for service in rows.into_iter().take(limit.unwrap_or(usize::MAX)) {
        append_service_row(&mut builder, service);
        if builder.num_rows() >= batch_size {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
        }
    }
    if builder.num_rows() > 0 {
        let result = builder.into_batch();
        let _ = tx.send(result).await;
    }
}
