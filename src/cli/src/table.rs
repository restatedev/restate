use crate::storage::v1::scan_request::Filter;
use crate::storage::v1::storage_client::StorageClient;
use crate::storage::v1::{Pair, Range, ScanRequest};
use crate::value::{is_value_field, value_field, value_to_typed};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::array::{ArrayRef, BinaryArray, PrimitiveArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type, UInt64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, ScalarValue, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures::{stream, FutureExt, StreamExt, TryStreamExt};
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor, Value};
use std::any::Any;
use std::convert::AsRef;
use std::fmt::Debug;
use std::future;
use std::sync::Arc;
use tracing::debug;

pub(crate) fn register(
    ctx: &SessionContext,
    client: StorageClient<tonic::transport::Channel>,
) -> Result<(), DataFusionError> {
    let descriptor_pool = DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")).as_slice(),
    )
    .expect("failed to decode file descriptor set");
    let key_desc = descriptor_pool
        .get_message_by_name("dev.restate.storage.scan.v1.Key")
        .expect("Key message must be present in descriptor pool");
    key_desc
        .clone()
        .fields()
        .filter(|f| f.containing_oneof().is_some())
        .filter_map(|field| match field.kind() {
            Kind::Message(table_descriptor) => Some(GrpcTableProvider::new(
                field.name().to_string(),
                descriptor_pool.clone(),
                table_descriptor,
                key_desc.clone(),
                client.clone(),
            )),
            _ => None,
        })
        .try_for_each(|provider| {
            ctx.register_table(provider.table_name().as_str(), Arc::new(provider))
                .map(|_| ())
        })
}

#[derive(Debug)]
struct GrpcTableProvider {
    table_name: String,
    descriptor_pool: DescriptorPool,
    key_desc: MessageDescriptor,
    client: StorageClient<tonic::transport::Channel>,
    schema: SchemaRef,
}

impl GrpcTableProvider {
    fn new(
        table_name: String,
        descriptor_pool: DescriptorPool,
        table_desc: MessageDescriptor,
        key_desc: MessageDescriptor,
        client: StorageClient<tonic::transport::Channel>,
    ) -> Self {
        let mut fields: Vec<_> = table_desc
            .fields()
            .map(|f| {
                Field::new(
                    f.name(),
                    match f.kind() {
                        Kind::Uint32 => DataType::UInt32,
                        Kind::Uint64 => DataType::UInt64,
                        Kind::Bytes => DataType::Binary,
                        Kind::String => DataType::Utf8,
                        typ => panic!("unimplemented datatype {typ:?} in field {}", f.name()),
                    },
                    false,
                )
            })
            .collect();
        fields.push(value_field(table_name.as_str()));

        Self {
            table_name,
            descriptor_pool,
            key_desc,
            client,
            schema: Arc::new(Schema::new(fields)),
        }
    }

    fn table_name(&self) -> String {
        self.table_name.clone()
    }
}

#[async_trait]
impl TableProvider for GrpcTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        let mut start = DynamicMessage::new(self.key_desc.clone());
        let mut end = DynamicMessage::new(self.key_desc.clone());

        // get_field_by_name_mut sets the structured key to default (ie all None fields) == no bounds
        start.get_field_by_name_mut(self.table_name.as_str());
        end.get_field_by_name_mut(self.table_name.as_str());

        for expr in filters {
            match expr {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    match (left.as_ref(), right.as_ref()) {
                        (Expr::Column(column), Expr::Literal(literal))
                        | (Expr::Literal(literal), Expr::Column(column)) => {
                            let literal = match literal {
                                ScalarValue::UInt32(Some(i)) => Value::U32(*i),
                                ScalarValue::UInt64(Some(i)) => Value::U64(*i),
                                ScalarValue::Utf8(Some(s)) => Value::String(s.clone()),
                                ScalarValue::Binary(Some(b)) => {
                                    Value::Bytes(Bytes::from(b.clone()))
                                }
                                _ => todo!("unsupported field type: {}", literal),
                            };
                            match op {
                                Operator::Lt | Operator::LtEq => {
                                    // if its Lt, we can still provide the closed bound and just let
                                    // datafusion cut off the last key if its present
                                    set_field(
                                        &mut end,
                                        self.table_name.as_str(),
                                        column.name.as_str(),
                                        literal.clone(),
                                    );
                                }
                                Operator::Gt | Operator::GtEq => {
                                    // if its Gt, we can still provide the closed bound and just let
                                    // datafusion cut off the first key if its present
                                    set_field(
                                        &mut start,
                                        self.table_name.as_str(),
                                        column.name.as_str(),
                                        literal.clone(),
                                    )
                                }
                                Operator::Eq => {
                                    set_field(
                                        &mut start,
                                        self.table_name.as_str(),
                                        column.name.as_str(),
                                        literal.clone(),
                                    );
                                    set_field(
                                        &mut end,
                                        self.table_name.as_str(),
                                        column.name.as_str(),
                                        literal.clone(),
                                    );
                                }
                                other => {
                                    return Err(DataFusionError::Internal(format!(
                                        "provided an operator {other:?} we don't support"
                                    )));
                                }
                            }
                        }
                        other => {
                            return Err(DataFusionError::Internal(format!(
                                "provided a binary expression {op:?} on {other:?} we don't support"
                            )));
                        }
                    }
                }
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "provided a filter expression {other:?} we don't support"
                    )));
                }
            }
        }

        let start = start
            .transcode_to()
            .expect("failed to build a start key message");
        let end = end
            .transcode_to()
            .expect("failed to build an end key message");

        let request = ScanRequest {
            filter: Some(Filter::Range(Range {
                start: Some(start),
                end: Some(end),
            })),
        };

        Ok(Arc::new(GrpcExec::new(
            self.table_name.clone(),
            projected_schema,
            self.descriptor_pool.clone(),
            self.key_desc.clone(),
            limit,
            self.client.clone(),
            request,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(filters
            .iter()
            .map(|expr| match expr {
                Expr::BinaryExpr(BinaryExpr {
                                     left,
                                     op: Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq,
                                     right,
                                 }) => match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(_) | Expr::Literal(_), Expr::Column(_) | Expr::Literal(_)) => {
                        // without knowing the key structure we can't guarantee exact results
                        // so we ask that datafusion always filters client side
                        TableProviderFilterPushDown::Inexact
                    }
                    _ => TableProviderFilterPushDown::Unsupported,
                },
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }
}

fn set_field(message: &mut DynamicMessage, table_name: &str, field_name: &str, value: Value) {
    message
        .get_field_by_name_mut(table_name)
        .and_then(|f| f.as_message_mut())
        .unwrap()
        .set_field_by_name(field_name, value)
}

#[derive(Debug)]
struct GrpcExec {
    name: String,
    schema: SchemaRef,
    descriptor_pool: DescriptorPool,
    key_desc: MessageDescriptor,
    limit: Option<usize>,
    request: ScanRequest,
    client: StorageClient<tonic::transport::Channel>,
}

impl GrpcExec {
    fn new(
        name: String,
        schema: SchemaRef,
        descriptor_pool: DescriptorPool,
        key_desc: MessageDescriptor,
        limit: Option<usize>,
        client: StorageClient<tonic::transport::Channel>,
        request: ScanRequest,
    ) -> Self {
        Self {
            name,
            schema,
            descriptor_pool,
            key_desc,
            limit,
            client,
            request,
        }
    }
}

impl ExecutionPlan for GrpcExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        debug!("executing {:?}", self.request);

        let mut client = self.client.clone();
        let request = self.request.clone();
        let resp = tokio::task::spawn(async move { client.scan(request).await });

        let name = self.name.clone();
        let descriptor_pool = self.descriptor_pool.clone();
        let key_desc = self.key_desc.clone();
        let schema = self.schema();
        let stream = resp
            .into_stream()
            .map(move |handle| match handle {
                Err(err) => {
                    // stream will have a single result, a join error
                    stream::once(future::ready(Err(DataFusionError::External(Box::new(err)))))
                        .boxed()
                }
                Ok(Err(err)) => {
                    // stream will have a single result, a grpc error
                    stream::once(future::ready(Err(DataFusionError::External(Box::new(err)))))
                        .boxed()
                }
                Ok(Ok(resp)) => {
                    // each result of the stream is a result from the grpc stream
                    let name = name.clone();
                    let descriptor_pool = descriptor_pool.clone();
                    let key_desc = key_desc.clone();
                    let schema = schema.clone();
                    resp.into_inner()
                        .try_chunks(ctx.session_config().batch_size())
                        .map(move |batch| {
                            batch
                                .map_err(|err| DataFusionError::External(Box::new(err)))
                                .map(|batch| {
                                    batch_to_record_batch(
                                        name.as_str(),
                                        descriptor_pool.clone(),
                                        key_desc.clone(),
                                        schema.clone(),
                                        batch,
                                    )
                                })
                        })
                        .boxed()
                }
            })
            .flatten();

        let stream = match self.limit {
            None => stream.boxed(),
            Some(limit) => stream
                .enumerate()
                .take_while(move |(i, _)| future::ready(i < &limit))
                .map(|(_, result)| result)
                .boxed(),
        };
        Ok(
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
                as SendableRecordBatchStream,
        )
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "GrpcExec: request={:?}", self.request)
            }
        }
    }
    fn statistics(&self) -> Statistics {
        Default::default()
    }
}

fn batch_to_record_batch(
    table_name: &str,
    descriptor_pool: DescriptorPool,
    key_desc: MessageDescriptor,
    schema: SchemaRef,
    batch: Vec<Pair>,
) -> RecordBatch {
    debug!("received batch with {} items", batch.len());
    if batch.is_empty() {
        return RecordBatch::new_empty(schema);
    }
    let (keys, values): (Vec<_>, Vec<_>) = batch
        .into_iter()
        .map(|pair| {
            let mut key = DynamicMessage::new(key_desc.clone());
            key.transcode_from(pair.key.as_ref().expect("key must be provided"))
                .expect("problem trancoding key");
            if let Value::Message(key) = key
                .get_field_by_name(table_name)
                .unwrap_or_else(|| panic!("{table_name} must be present in key oneof"))
                .as_ref()
            {
                (key.clone(), pair.value)
            } else {
                panic!("{table_name} must be a message")
            }
        })
        .unzip();

    let columns = schema.fields.iter().map(|field| {
        if is_value_field(field) {
            return value_to_typed(descriptor_pool.clone(), field, values.iter());
        }

        let items = keys
            .iter()
            .map(|item| item.get_field_by_name(field.name().as_str()).expect(""));

        match field.data_type() {
            DataType::UInt64 => Arc::new(PrimitiveArray::<UInt64Type>::from_iter_values(items.map(
                |field| {
                    field
                        .as_u64()
                        .unwrap_or_else(|| panic!("field {} type must match schema type", field))
                },
            ))) as ArrayRef,
            DataType::UInt32 => Arc::new(PrimitiveArray::<UInt32Type>::from_iter_values(items.map(
                |field| {
                    field
                        .as_u32()
                        .unwrap_or_else(|| panic!("field {} type must match schema type", field))
                },
            ))) as ArrayRef,
            DataType::Utf8 => Arc::new(StringArray::from_iter_values(items.map(|field| {
                field
                    .as_str()
                    .unwrap_or_else(|| panic!("field {} type must match schema type", field))
                    .to_owned()
            }))) as ArrayRef,
            DataType::Binary => Arc::new(BinaryArray::from_iter_values(items.map(|field| {
                field
                    .as_bytes()
                    .unwrap_or_else(|| panic!("field {} type must match schema type", field))
                    .to_owned()
            }))) as ArrayRef,
            _ => todo!("unsupported field type"),
        }
    });

    RecordBatch::try_new(schema.clone(), columns.collect()).expect("failed to create batch")
}
