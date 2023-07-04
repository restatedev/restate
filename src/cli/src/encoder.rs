use std::any::Any;
use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures::StreamExt;
use prost_reflect::DescriptorPool;

use crate::field::ExtendedField;

#[derive(Debug)]
pub(crate) struct EncoderExec {
    descriptor_pool: DescriptorPool,
    encoded_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
}

impl EncoderExec {
    pub(crate) fn new(
        descriptor_pool: DescriptorPool,
        encoded_schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            descriptor_pool,
            encoded_schema,
            input,
        }
    }
}

impl ExecutionPlan for EncoderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.encoded_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(EncoderExec::new(
            self.descriptor_pool.clone(),
            self.encoded_schema.clone(),
            children[0].clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let descriptor_pool = self.descriptor_pool.clone();
        let schema = self.schema();
        let decoded_schema = self.input.schema();
        let stream = self.input.execute(partition, ctx)?.map(move |batch| {
            let batch = batch?;

            let mut columns = Vec::with_capacity(batch.num_columns());

            for field in &decoded_schema.fields {
                let name = field.name();
                let field: ExtendedField = (**field).clone().into();
                columns.push(field.encode(
                    descriptor_pool.clone(),
                    batch.column_by_name(name).unwrap().clone(),
                ));
            }

            Ok(RecordBatch::try_new(schema.clone(), columns)?)
        });

        Ok(
            Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream))
                as SendableRecordBatchStream,
        )
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "EncoderExec")
            }
        }
    }
    fn statistics(&self) -> Statistics {
        Default::default()
    }
}
