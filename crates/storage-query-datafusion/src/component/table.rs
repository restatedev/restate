// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::schema::ComponentBuilder;

use crate::component::row::append_component_row;
use crate::context::QueryContext;
use crate::generic_table::{GenericTableProvider, RangeScanner};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
pub use datafusion_expr::UserDefinedLogicalNode;
use restate_schema_api::component::{ComponentMetadata, ComponentMetadataResolver};
use restate_types::identifiers::PartitionKey;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub(crate) fn register_self(
    ctx: &QueryContext,
    resolver: impl ComponentMetadataResolver + Send + Sync + Debug + 'static,
) -> datafusion::common::Result<()> {
    let component_table = GenericTableProvider::new(
        ComponentBuilder::schema(),
        Arc::new(ComponentMetadataScanner(resolver)),
    );

    ctx.as_ref()
        .register_table("sys_component", Arc::new(component_table))
        .map(|_| ())
}

#[derive(Debug, Clone)]
struct ComponentMetadataScanner<SMR>(SMR);

/// TODO This trait makes little sense for sys_service,
///  but it's fine nevertheless as the caller always uses the full range
impl<SMR: ComponentMetadataResolver + Debug + Sync + Send + 'static> RangeScanner
    for ComponentMetadataScanner<SMR>
{
    fn scan(
        &self,
        _range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 16);
        let tx = stream_builder.tx();

        let rows = self.0.list_components();
        stream_builder.spawn(async move {
            for_each_state(schema, tx, rows).await;
            Ok(())
        });
        stream_builder.build()
    }
}

async fn for_each_state(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: Vec<ComponentMetadata>,
) {
    let mut builder = ComponentBuilder::new(schema.clone());
    let mut temp = String::new();
    for component in rows {
        append_component_row(&mut builder, &mut temp, component);
        if builder.full() {
            let batch = builder.finish();
            if tx.send(Ok(batch)).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = ComponentBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(Ok(result)).await;
    }
}
