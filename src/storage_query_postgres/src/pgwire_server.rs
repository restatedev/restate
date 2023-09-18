// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use datafusion::arrow::array::{Array, AsArray};
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
use datafusion::common::cast::{as_date32_array, as_date64_array};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{stream, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::extended_query::NoopExtendedQueryHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use pgwire::types::ToSqlText;
use postgres_types::{IsNull, ToSql};
use restate_storage_query_datafusion::context::QueryContext;
use tracing::warn;

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

    pub fn spawn_connection(&self, incoming_socket: TcpStream, addr: SocketAddr) {
        let authenticator_ref = self.authenticator.make();
        let processor_ref = self.processor.make();
        let placeholder_ref = self.placeholder.make();
        tokio::spawn(async move {
            let result = process_socket(
                incoming_socket,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await;

            if let Err(err) = result {
                warn!("Failed processing socket for connection '{addr}': {err}");
            }
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
        DataType::Date32 => Type::DATE,
        DataType::Date64 => Type::TIMESTAMP,
        DataType::Binary => Type::BYTEA,
        DataType::LargeBinary => Type::BYTEA,
        DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 => Type::VARCHAR,
        DataType::LargeUtf8 => Type::VARCHAR,
        DataType::List(field) => match field.data_type() {
            DataType::Utf8 => Type::VARCHAR_ARRAY,
            _ => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Unsupported Datatype {df_type}"),
                ))));
            }
        },
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
                let format = match f.data_type() {
                    DataType::Binary | DataType::LargeBinary => FieldFormat::Binary,
                    _ => FieldFormat::Text,
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

fn encode_value(
    encoder: &mut DataRowEncoder,
    arr: &Arc<dyn Array>,
    idx: usize,
) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Boolean => encoder.encode_field(&arr.as_boolean().value(idx))?,
        DataType::Int8 => encoder.encode_field(&arr.as_primitive::<Int8Type>().value(idx))?,
        DataType::Int16 => encoder.encode_field(&arr.as_primitive::<Int16Type>().value(idx))?,
        DataType::Int32 => encoder.encode_field(&arr.as_primitive::<Int32Type>().value(idx))?,
        DataType::Int64 => encoder.encode_field(&arr.as_primitive::<Int64Type>().value(idx))?,
        DataType::UInt32 => encoder.encode_field(&arr.as_primitive::<UInt32Type>().value(idx))?,
        DataType::UInt64 => {
            encoder.encode_field(&(arr.as_primitive::<UInt64Type>().value(idx) as i64))?
        }
        DataType::Float32 => encoder.encode_field(&arr.as_primitive::<Float32Type>().value(idx))?,
        DataType::Float64 => encoder.encode_field(&arr.as_primitive::<Float64Type>().value(idx))?,
        DataType::Utf8 => encoder.encode_field(&arr.as_string::<i32>().value(idx))?,
        DataType::LargeUtf8 => encoder.encode_field(&arr.as_string::<i64>().value(idx))?,
        DataType::Binary => encoder.encode_field(&arr.as_binary::<i32>().value(idx))?,
        DataType::LargeBinary => encoder.encode_field(&arr.as_binary::<i64>().value(idx))?,
        DataType::Date64 => encoder.encode_field(
            &as_date64_array(arr)
                .unwrap()
                .value_as_datetime(idx)
                .unwrap(),
        )?,
        DataType::Date32 => encoder.encode_field(
            &as_date32_array(arr)
                .unwrap()
                .value_as_datetime(idx)
                .unwrap(),
        )?,
        DataType::List(_) => {
            let arr = &arr.as_list::<i32>().value(idx);
            match arr.data_type() {
                DataType::Utf8 => {
                    encoder.encode_field(&SQLVec(arr.as_string::<i32>().iter().collect()))?;
                }
                DataType::LargeUtf8 => {
                    encoder.encode_field(&SQLVec(arr.as_string::<i64>().iter().collect()))?;
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
                    ))))
                }
            }
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
            ))))
        }
    }
    Ok(())
}

#[derive(Debug)]
struct SQLVec<T>(Vec<T>);

impl<T: ToSqlText> ToSqlText for SQLVec<T> {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_slice(b"{");

        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                out.put_slice(b",");
            }
            match v.to_sql_text(ty, out)? {
                IsNull::Yes => out.put_slice(b"NULL"),
                IsNull::No => (),
            }
        }

        out.put_slice(b"}");

        Ok(IsNull::No)
    }
}

impl<T: ToSql> ToSql for SQLVec<T> {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool
    where
        Self: Sized,
    {
        Vec::<T>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.0.to_sql_checked(ty, out)
    }
}
