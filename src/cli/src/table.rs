use std::any::Any;
use std::convert::AsRef;
use std::fmt::Debug;
use std::future;
use std::sync::Arc;

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
use futures::{stream, FutureExt, StreamExt};
use once_cell::sync::Lazy;
use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MessageDescriptor, ReflectMessage, Value,
};
use tracing::info;

use crate::storage::v1::storage_client::StorageClient;
use crate::storage::v1::Batch;
use crate::value::{is_value_field, value_field, value_to_typed};

pub(crate) static DESCRIPTOR_POOL: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")).as_slice(),
    )
    .unwrap()
});

#[derive(Message, ReflectMessage)]
#[prost_reflect(
    descriptor_pool = "DESCRIPTOR_POOL",
    message_name = "dev.restate.storage.scan.v1.Key"
)]
pub struct Key {}

#[derive(Clone, Message, ReflectMessage)]
#[prost_reflect(
    descriptor_pool = "DESCRIPTOR_POOL",
    message_name = "dev.restate.storage.scan.v1.ScanRequest"
)]
pub struct ScanRequest {}

static SCAN_REQUEST_DESC: Lazy<MessageDescriptor> = Lazy::new(|| ScanRequest {}.descriptor());
static KEY_DESC: Lazy<MessageDescriptor> = Lazy::new(|| Key {}.descriptor());

pub(crate) fn register(
    ctx: &SessionContext,
    client: StorageClient<tonic::transport::Channel>,
) -> Result<(), DataFusionError> {
    KEY_DESC
        .fields()
        .filter(|f| f.containing_oneof().is_some())
        .filter_map(|field| match field.kind() {
            Kind::Message(descriptor) => Some(GrpcTableProvider::new(
                field.name().to_string(),
                descriptor.clone(),
                client.clone(),
            )),
            _ => None,
        })
        .try_for_each(|provider| {
            ctx.register_table(provider.name().as_str(), Arc::new(provider))
                .map(|_| ())
        })
}

#[derive(Debug)]
pub struct GrpcTableProvider {
    name: String,
    client: StorageClient<tonic::transport::Channel>,
    schema: SchemaRef,
}

impl GrpcTableProvider {
    pub fn new(
        name: String,
        desc: MessageDescriptor,
        client: StorageClient<tonic::transport::Channel>,
    ) -> Self {
        let mut fields: Vec<_> = desc
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
        fields.push(value_field(name.as_str()));

        Self {
            name,
            client,
            schema: Arc::new(Schema::new(fields)),
        }
    }

    pub fn name(&self) -> String {
        return self.name.clone();
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
            Some(p) => Arc::new(self.schema.project(&p)?),
            None => self.schema.clone(),
        };

        let mut request_message = DynamicMessage::new(SCAN_REQUEST_DESC.clone());
        let range_message = request_message
            .get_field_by_name_mut("range")
            .and_then(|f| f.as_message_mut())
            .unwrap();
        let set_field =
            |key_name: &str, field_name: &str, value: Value, range_message: &mut DynamicMessage| {
                range_message
                    .get_field_by_name_mut(key_name)
                    .and_then(|f| f.as_message_mut())
                    .and_then(|m| m.get_field_by_name_mut(self.name.as_str()))
                    .and_then(|f| f.as_message_mut())
                    .unwrap()
                    .set_field_by_name(field_name, value)
            };

        for expr in filters {
            match expr {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    match (left.as_ref(), right.as_ref()) {
                        (Expr::Column(column), Expr::Literal(literal))
                        | (Expr::Literal(literal), Expr::Column(column)) => {
                            let literal = match literal {
                                ScalarValue::UInt32(Some(i)) => Value::U32(i.clone()),
                                ScalarValue::UInt64(Some(i)) => Value::U64(i.clone()),
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
                                        "end",
                                        column.name.as_str(),
                                        literal.clone(),
                                        range_message,
                                    );
                                }
                                Operator::Gt | Operator::GtEq => {
                                    // if its Gt, we can still provide the closed bound and just let
                                    // datafusion cut off the first key if its present
                                    set_field(
                                        "start",
                                        column.name.as_str(),
                                        literal.clone(),
                                        range_message,
                                    );
                                }
                                Operator::Eq => {
                                    set_field(
                                        "start",
                                        column.name.as_str(),
                                        literal.clone(),
                                        range_message,
                                    );
                                    set_field(
                                        "end",
                                        column.name.as_str(),
                                        literal.clone(),
                                        range_message,
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

        if !range_message.has_field_by_name("start") && !range_message.has_field_by_name("end") {
            // no filters; we still need to provide a zero valued start message to communicate which table we are querying
            // fortunately get_field_by_name_mut automatically inserts the default for us
            range_message
                .get_field_by_name_mut("start")
                .and_then(|field| field.as_message_mut())
                .and_then(|m| m.get_field_by_name_mut(self.name.as_str()));
        }

        Ok(Arc::new(GrpcExec::new(
            self.name.clone(),
            projected_schema,
            limit,
            self.client.clone(),
            request_message.transcode_to().unwrap(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        info!("filters: {filters:?}");
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

#[derive(Debug)]
struct GrpcExec {
    name: String,
    schema: SchemaRef,
    limit: Option<usize>,
    request: crate::storage::v1::ScanRequest,
    client: StorageClient<tonic::transport::Channel>,
}

impl GrpcExec {
    fn new(
        name: String,
        schema: SchemaRef,
        limit: Option<usize>,
        client: crate::storage::v1::storage_client::StorageClient<tonic::transport::Channel>,
        request: crate::storage::v1::ScanRequest,
    ) -> Self {
        Self {
            name,
            schema,
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
        // TODO maybe
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        info!("executing {:?}", self.request);

        let mut client = self.client.clone();
        let request = self.request.clone();
        let resp = tokio::task::spawn(async move { client.scan(request).await });

        let name = self.name.clone();
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
                    let schema = schema.clone();
                    resp.into_inner()
                        .map(move |batch| {
                            batch
                                .map_err(|err| DataFusionError::External(Box::new(err)))
                                .map(|batch| {
                                    batch_to_record_batch(name.as_str(), schema.clone(), batch)
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

fn batch_to_record_batch(table_name: &str, schema: SchemaRef, batch: Batch) -> RecordBatch {
    info!("received batch with {} items", batch.items.len());
    if batch.items.len() == 0 {
        return RecordBatch::new_empty(schema.clone());
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields.len());
    let mut keys = Vec::with_capacity(batch.items.len());
    for item in &batch.items {
        let mut dynamic = DynamicMessage::new(KEY_DESC.clone());
        dynamic
            .transcode_from(item.key.as_ref().expect("key must be provided"))
            .expect("problem trancoding key");
        if let Value::Message(item) = dynamic
            .get_field_by_name(table_name)
            .expect(format!("{table_name} must be present in key oneof").as_str())
            .as_ref()
        {
            keys.push(item.clone())
        } else {
            panic!("{table_name} must be a message")
        }
    }

    let mut value_field: Option<String> = None;

    for field in &schema.fields {
        if is_value_field(field) {
            value_field = Some(field.name().clone());
            continue;
        }
        let items = keys
            .iter()
            .map(|item| item.get_field_by_name(field.name().as_str()).expect(""));

        columns.push(match field.data_type() {
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
        });
    }

    value_field
        .and_then(|name| schema.column_with_name(name.as_str()))
        .map(|(i, field)| {
            columns.insert(i, value_to_typed(field, batch));
        });

    RecordBatch::try_new(schema.clone(), columns).expect("failed to create batch")
}
