// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, LargeBinaryArray, LargeStringArray,
    PrimitiveArray, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::Float32Type;
use datafusion::arrow::datatypes::Float64Type;
use datafusion::arrow::datatypes::Int16Type;
use datafusion::arrow::datatypes::Int32Type;
use datafusion::arrow::datatypes::Int64Type;
use datafusion::arrow::datatypes::Int8Type;
use datafusion::arrow::datatypes::UInt32Type;
use datafusion::arrow::datatypes::UInt64Type;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::temporal_conversions::{date32_to_datetime, date64_to_datetime};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{stream, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::context::QueryContext;
use crate::extended_query::NoopExtendedQueryHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;

pub(crate) struct HandlerFactory {
    processor: Arc<StatelessMakeHandler<DfSessionService>>,
    placeholder: Arc<StatelessMakeHandler<NoopExtendedQueryHandler>>,
    authenticator: Arc<StatelessMakeHandler<NoopStartupHandler>>,
}

impl HandlerFactory {
    pub fn new(ctx: QueryContext) -> Self {
        let processor = Arc::new(StatelessMakeHandler::new(Arc::new(DfSessionService::new(
            ctx,
        ))));
        // We have not implemented extended query in this server, use placeholder instead
        let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
            NoopExtendedQueryHandler::new(),
        )));
        let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

        Self {
            processor,
            placeholder,
            authenticator,
        }
    }

    pub fn spawn_connection(&self, incoming_socket: TcpStream) {
        let authenticator_ref = self.authenticator.make();
        let processor_ref = self.processor.make();
        let placeholder_ref = self.placeholder.make();
        tokio::spawn(async move {
            let _ = process_socket(
                incoming_socket,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await;
        });
    }
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
    async fn do_query<'a, C>(&self, _client: &C, query: &'a str) -> PgWireResult<Vec<Response<'a>>>
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
        DataType::Date32 | DataType::Date64 => Type::VARCHAR,
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

fn get_date64_value(arr: &Arc<dyn Array>, idx: usize) -> String {
    let value = arr
        .as_any()
        .downcast_ref::<Date64Array>()
        .unwrap()
        .value(idx);

    let dt = date64_to_datetime(value).unwrap();
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn get_date32_value(arr: &Arc<dyn Array>, idx: usize) -> String {
    let value = arr
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap()
        .value(idx);

    let dt = date32_to_datetime(value).unwrap();
    dt.format("%Y-%m-%d %H:%M:%S").to_string()
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
        DataType::Date64 => encoder.encode_field(&get_date64_value(arr, idx))?,
        DataType::Date32 => encoder.encode_field(&get_date32_value(arr, idx))?,
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!(
                    "Unsupported Datatype {} and array {:?}",
                    arr.data_type(),
                    &arr
                ),
            ))))
        }
    }
    Ok(())
}
