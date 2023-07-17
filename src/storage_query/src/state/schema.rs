#![allow(dead_code)]

use crate::table_macro::*;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, Int32Builder, LargeBinaryBuilder, LargeStringBuilder, UInt32Builder,
};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

define_table!(state(
    partition_key: DataType::UInt32,
    service: DataType::LargeUtf8,
    service_key: DataType::LargeBinary,
    service_key_utf8: DataType::LargeUtf8,
    service_key_int32: DataType::Int32,
    service_key_uuid: DataType::LargeUtf8,
    key: DataType::LargeUtf8,
    value_utf8: DataType::LargeUtf8,
    value: DataType::LargeBinary,
));
