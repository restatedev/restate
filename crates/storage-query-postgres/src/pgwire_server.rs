// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, LargeBinaryArray, LargeStringArray, PrimitiveArray,
    StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::Float32Type;
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::datatypes::Int8Type;
use datafusion::arrow::datatypes::Int16Type;
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::datatypes::Int64Type;
use datafusion::arrow::datatypes::UInt32Type;
use datafusion::arrow::datatypes::UInt64Type;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{Sink, SinkExt, StreamExt, stream};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::NoticeResponse;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;

use crate::extended_query::NoopExtendedQueryHandler;
use restate_core::{TaskCenter, TaskKind};
use restate_datafusion::context::QueryContext;

pub(crate) struct HandlerFactory {
    processor: Arc<DfSessionService>,
    placeholder: Arc<NoopExtendedQueryHandler>,
    authenticator: Arc<NoAuthHandler>,
    copy_handler: Arc<NoopCopyHandler>,
}

impl PgWireServerHandlers for HandlerFactory {
    type StartupHandler = NoAuthHandler;
    type SimpleQueryHandler = DfSessionService;
    type ExtendedQueryHandler = NoopExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.processor.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.placeholder.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.authenticator.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        self.copy_handler.clone()
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

pub(crate) struct NoAuthHandler {}

#[async_trait]
impl NoopStartupHandler for NoAuthHandler {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: pgwire::messages::PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<pgwire::messages::PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::new(
                vec![
                    (
                        b'S', // severity
                        "WARNING".into(),
                    ),
                    (
                        b'V', // severity again
                        "WARNING".into(),
                    ),
                    (
                        b'C', // error code
                        "01000".into(),
                    ),
                    (
                        b'M', // message
                        "Restate's :9071 PSQL endpoint is deprecated and will be removed in a future release."
                            .into(),
                    ),
                    (
                        b'H', // hint
                        "You can make SQL queries using the CLI or with the :9070/query API."
                            .into(),
                    )
                ],
            )))
            .await?;

        Ok(())
    }
}

impl HandlerFactory {
    pub fn new(ctx: QueryContext) -> Self {
        let processor = Arc::new(DfSessionService::new(ctx));
        // We have not implemented extended query in this server, use placeholder instead
        let placeholder = Arc::new(NoopExtendedQueryHandler::new());
        let authenticator = Arc::new(NoAuthHandler {});
        let copy_handler = Arc::new(NoopCopyHandler);

        Self {
            processor,
            placeholder,
            authenticator,
            copy_handler,
        }
    }
}

pub fn spawn_connection(
    factory: Arc<HandlerFactory>,
    incoming_socket: TcpStream,
    addr: SocketAddr,
) {
    // fails only if we are shutting down
    let _ = TaskCenter::spawn_child(
        TaskKind::SocketHandler,
        "postgres-query-connection",
        async move {
            let result = process_socket(incoming_socket, None, factory).await;
            result.context(format!("Failed processing socket for connection '{addr}'"))
        },
    );
}

pub struct DfSessionService {
    session_context: Mutex<QueryContext>,
}

impl DfSessionService {
    pub fn new(ctx: QueryContext) -> DfSessionService {
        DfSessionService {
            session_context: Mutex::new(ctx),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for DfSessionService {
    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let ctx = self.session_context.lock().await;
        let df = ctx
            .execute(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        let resp = arrow_to_pg_encoder(df).await?;
        Ok(vec![Response::Query(resp)])
    }
}

fn into_pg_type(df_type: &DataType) -> PgWireResult<Type> {
    Ok(match df_type {
        DataType::Null => Type::UNKNOWN,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::CHAR,
        DataType::Int16 => Type::INT2,
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::UInt8 => Type::CHAR,
        DataType::UInt16 => Type::INT2,
        DataType::UInt32 => Type::INT4,
        DataType::UInt64 => Type::INT8,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Binary => Type::BYTEA,
        DataType::LargeBinary => Type::BYTEA,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::VARCHAR,
        DataType::LargeUtf8 => Type::VARCHAR,
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {df_type}"),
            ))));
        }
    })
}

async fn arrow_to_pg_encoder<'a>(
    recordbatch_stream: SendableRecordBatchStream,
) -> PgWireResult<QueryResponse<'a>> {
    let schema = recordbatch_stream.schema();
    let fields = Arc::new(
        schema
            .fields()
            .iter()
            .map(|f| {
                let pg_type = into_pg_type(f.data_type())?;
                let format = if matches!(f.data_type(), DataType::Binary) {
                    FieldFormat::Binary
                } else {
                    FieldFormat::Text
                };

                Ok(FieldInfo::new(f.name().into(), None, None, pg_type, format))
            })
            .collect::<PgWireResult<Vec<FieldInfo>>>()?,
    );

    let fields_ref = fields.clone();
    let pg_row_stream = recordbatch_stream
        .map(move |rb: datafusion::error::Result<RecordBatch>| {
            if let Err(e) = rb {
                let pg_err = Err(PgWireError::ApiError(Box::new(e)));
                return stream::iter(vec![pg_err]);
            }

            let rb = rb.unwrap();
            let rows = rb.num_rows();
            let mut results = Vec::with_capacity(rows);

            for row in 0..rows {
                let row_result = encode_row(row, &rb, &fields_ref);
                results.push(row_result);
            }

            stream::iter(results)
        })
        .flatten();

    Ok(QueryResponse::new(fields, pg_row_stream))
}

fn encode_row(
    row: usize,
    rb: &RecordBatch,
    fields_ref: &Arc<Vec<FieldInfo>>,
) -> PgWireResult<DataRow> {
    let cols = rb.num_columns();
    let mut encoder = DataRowEncoder::new(Arc::clone(fields_ref));
    for col in 0..cols {
        let array = rb.column(col);
        if array.is_null(row) {
            encoder.encode_field(&None::<i8>)?;
        } else {
            encode_value(&mut encoder, array, row)?;
        }
    }
    encoder.finish()
}

fn get_bool_value(arr: &Arc<dyn Array>, idx: usize) -> bool {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(idx)
}

fn get_timestamp_value(arr: &Arc<dyn Array>, idx: usize) -> SystemTime {
    let value = arr
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap()
        .value(idx);

    SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_millis(value as u64))
        .unwrap()
}

macro_rules! get_primitive_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> $pt {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .value(idx)
        }
    };
}

get_primitive_value!(get_i8_value, Int8Type, i8);
get_primitive_value!(get_i16_value, Int16Type, i16);
get_primitive_value!(get_i32_value, Int32Type, i32);
get_primitive_value!(get_i64_value, Int64Type, i64);
get_primitive_value!(get_u32_value, UInt32Type, u32);
get_primitive_value!(get_u64_value, UInt64Type, u64);
get_primitive_value!(get_f32_value, Float32Type, f32);
get_primitive_value!(get_f64_value, Float64Type, f64);

fn get_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> &str {
    arr.as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(idx)
}

fn get_large_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> &str {
    arr.as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .value(idx)
}

fn get_binary_value(arr: &Arc<dyn Array>, idx: usize) -> &[u8] {
    arr.as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(idx)
}

fn get_large_binary_value(arr: &Arc<dyn Array>, idx: usize) -> &[u8] {
    arr.as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap()
        .value(idx)
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    arr: &Arc<dyn Array>,
    idx: usize,
) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Boolean => encoder.encode_field(&get_bool_value(arr, idx))?,
        DataType::Int8 => encoder.encode_field(&get_i8_value(arr, idx))?,
        DataType::Int16 => encoder.encode_field(&get_i16_value(arr, idx))?,
        DataType::Int32 => encoder.encode_field(&get_i32_value(arr, idx))?,
        DataType::Int64 => encoder.encode_field(&get_i64_value(arr, idx))?,
        DataType::UInt32 => encoder.encode_field(&get_u32_value(arr, idx))?,
        DataType::UInt64 => encoder.encode_field(&(get_u64_value(arr, idx) as i64))?,
        DataType::Float32 => encoder.encode_field(&get_f32_value(arr, idx))?,
        DataType::Float64 => encoder.encode_field(&get_f64_value(arr, idx))?,
        DataType::Utf8 => encoder.encode_field(&get_utf8_value(arr, idx))?,
        DataType::LargeUtf8 => encoder.encode_field(&get_large_utf8_value(arr, idx))?,
        DataType::Binary => encoder.encode_field(&get_binary_value(arr, idx))?,
        DataType::LargeBinary => encoder.encode_field(&get_large_binary_value(arr, idx))?,
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            encoder.encode_field(&get_timestamp_value(arr, idx))?
        }
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!(
                    "Unsupported Datatype {} and array {:?}",
                    arr.data_type(),
                    &arr
                ),
            ))));
        }
    }
    Ok(())
}
