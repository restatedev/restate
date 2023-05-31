#![allow(dead_code)]

use crate::table_macro::*;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, Date64Builder, Int32Builder, LargeBinaryBuilder, LargeStringBuilder,
    UInt32Builder,
};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

define_table!(status(
    partition_key: DataType::UInt32,
    service: DataType::LargeUtf8,
    method: DataType::LargeUtf8,
    service_key: DataType::LargeBinary,
    service_key_utf8: DataType::LargeUtf8,
    service_key_int32: DataType::Int32,
    service_key_uuid: DataType::LargeUtf8,
    status: DataType::LargeUtf8,
    invocation_id: DataType::LargeUtf8,
    sid: DataType::LargeUtf8,
    invoked_by: DataType::LargeUtf8,
    invoked_by_service: DataType::LargeUtf8,
    invoked_by_sid: DataType::LargeUtf8,
    trace_id: DataType::LargeUtf8,
    journal_size: DataType::UInt32,
    created_at: DataType::Date64,
    modified_at: DataType::Date64,
));
