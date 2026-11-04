// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! DataFusion Scalar UDFs for querying binary Protobuf state without JSON overhead:
//! - `proto_get_str(service_name, state_key, value, field_path) -> LargeUtf8`
//! - `proto_get_int(service_name, state_key, value, field_path) -> Int64`
//! - `proto_get_float(service_name, state_key, value, field_path) -> Float64`
//! - `proto_get_bool(service_name, state_key, value, field_path) -> Boolean`
//! - `proto_to_json(service_name, state_key, value) -> LargeUtf8`

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanBuilder, Float64Builder, Int64Builder, LargeBinaryArray,
    LargeStringArray, LargeStringBuilder, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use restate_types::schema::service::ServiceMetadataResolver;

use super::DynamicProtoSchemaRegistry;
use super::wire::{extract_bool, extract_f64, extract_i64, extract_str, transcode_to_json};
use crate::context::QueryContext;
type ResolvedSteps = Option<Arc<Vec<super::PathStep>>>;
type ResolvedStrPath = Option<(Arc<super::ServiceProtoSchemas>, Arc<Vec<super::PathStep>>)>;
type ResolvedMsgSchema = Option<(
    Arc<super::ServiceProtoSchemas>,
    Arc<super::CompiledMessageSchema>,
)>;

pub fn register_udfs<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    ctx: &QueryContext,
    registry: DynamicProtoSchemaRegistry<SMR>,
) {
    let session = ctx.as_ref();
    session.register_udf(make_proto_get_str_udf(registry.clone()));
    session.register_udf(make_proto_get_int_udf(registry.clone()));
    session.register_udf(make_proto_get_float_udf(registry.clone()));
    session.register_udf(make_proto_get_bool_udf(registry.clone()));
    session.register_udf(make_proto_to_json_udf(registry));
}

#[inline]
fn scalar_str(arg: &ColumnarValue) -> Option<&str> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
            Some(s.as_str())
        }
        _ => None,
    }
}

#[inline]
fn uniform_batch_str<'a>(arg: &'a ColumnarValue, buf: &'a Option<ArrayRef>) -> Option<&'a str> {
    if let Some(s) = scalar_str(arg) {
        return Some(s);
    }
    let arr = buf.as_ref()?;
    if arr.is_empty() || arr.null_count() > 0 {
        return None;
    }
    if let Some(s_arr) = arr.as_any().downcast_ref::<LargeStringArray>() {
        let first = s_arr.value(0);
        let last = s_arr.value(s_arr.len() - 1);
        if first == last {
            return Some(first);
        }
    } else if let Some(s_arr) = arr.as_any().downcast_ref::<StringArray>() {
        let first = s_arr.value(0);
        let last = s_arr.value(s_arr.len() - 1);
        if first == last {
            return Some(first);
        }
    }
    None
}

#[inline]
fn extract_str_arg<'a>(
    arg: &'a ColumnarValue,
    row_idx: usize,
    buf: &'a Option<ArrayRef>,
) -> Option<&'a str> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s))) => {
            Some(s.as_str())
        }
        ColumnarValue::Array(_) => {
            let arr = buf.as_ref()?;
            if arr.is_null(row_idx) {
                return None;
            }
            if let Some(s_arr) = arr.as_any().downcast_ref::<LargeStringArray>() {
                Some(s_arr.value(row_idx))
            } else if let Some(s_arr) = arr.as_any().downcast_ref::<StringArray>() {
                Some(s_arr.value(row_idx))
            } else {
                None
            }
        }
        _ => None,
    }
}

#[inline]
fn extract_bytes_arg<'a>(
    arg: &'a ColumnarValue,
    row_idx: usize,
    buf: &'a Option<ArrayRef>,
) -> Option<&'a [u8]> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b))) => {
            Some(b.as_slice())
        }
        ColumnarValue::Array(_) => {
            let arr = buf.as_ref()?;
            if arr.is_null(row_idx) {
                return None;
            }
            if let Some(b_arr) = arr.as_any().downcast_ref::<LargeBinaryArray>() {
                Some(b_arr.value(row_idx))
            } else if let Some(b_arr) = arr.as_any().downcast_ref::<BinaryArray>() {
                Some(b_arr.value(row_idx))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn to_array_opt(cv: &ColumnarValue) -> Option<ArrayRef> {
    match cv {
        ColumnarValue::Array(a) => Some(a.clone()),
        ColumnarValue::Scalar(_) => None,
    }
}

fn batch_row_count(args: &[ColumnarValue]) -> usize {
    for arg in args {
        if let ColumnarValue::Array(arr) = arg {
            return arr.len();
        }
    }
    1
}

fn make_proto_get_str_udf<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        if args.len() != 4 {
            return Err(DataFusionError::Execution(
                "proto_get_str expects 4 arguments: (service_name, state_key, value, field_path)"
                    .to_string(),
            ));
        }
        let num_rows = batch_row_count(args);
        let a0 = to_array_opt(&args[0]);
        let a1 = to_array_opt(&args[1]);
        let a2 = to_array_opt(&args[2]);
        let a3 = to_array_opt(&args[3]);

        let mut builder = LargeStringBuilder::with_capacity(num_rows, num_rows * 24);

        // Vectorized fast path when (service_name, state_key, field_path) are uniform across the batch
        // and `value` is a `LargeBinaryArray`.
        if let (Some(svc_name), Some(state_key), Some(field_path), Some(val_arr)) = (
            uniform_batch_str(&args[0], &a0),
            uniform_batch_str(&args[1], &a1),
            scalar_str(&args[3]),
            a2.as_ref()
                .and_then(|a| a.as_any().downcast_ref::<LargeBinaryArray>()),
        ) {
            let resolved = registry
                .resolve_service_schemas(svc_name)
                .and_then(|svc_schemas| {
                    let msg = svc_schemas.resolve_state_key(state_key)?;
                    let steps = msg.resolve_path(field_path, &svc_schemas.pool)?;
                    Some((svc_schemas, steps))
                });
            if let Some((svc_schemas, steps)) = resolved {
                let pool = &svc_schemas.pool;
                let steps_slice = steps.as_slice();
                if val_arr.null_count() == 0 {
                    for i in 0..num_rows {
                        if let Some(val_str) = extract_str(val_arr.value(i), steps_slice, pool) {
                            builder.append_value(val_str.as_ref());
                        } else {
                            builder.append_null();
                        }
                    }
                } else {
                    for i in 0..num_rows {
                        if val_arr.is_null(i) {
                            builder.append_null();
                        } else if let Some(val_str) =
                            extract_str(val_arr.value(i), steps_slice, pool)
                        {
                            builder.append_value(val_str.as_ref());
                        } else {
                            builder.append_null();
                        }
                    }
                }
            } else {
                for _ in 0..num_rows {
                    builder.append_null();
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        let mut last_lookup: Option<(&str, &str, &str, ResolvedStrPath)> = None;

        for i in 0..num_rows {
            let (Some(svc_name), Some(state_key), Some(bytes), Some(field_path)) = (
                extract_str_arg(&args[0], i, &a0),
                extract_str_arg(&args[1], i, &a1),
                extract_bytes_arg(&args[2], i, &a2),
                extract_str_arg(&args[3], i, &a3),
            ) else {
                builder.append_null();
                continue;
            };

            let resolved = match &last_lookup {
                Some((s, k, p, cached))
                    if *s == svc_name && *k == state_key && *p == field_path =>
                {
                    cached.clone()
                }
                _ => {
                    let r = registry
                        .resolve_service_schemas(svc_name)
                        .and_then(|svc_schemas| {
                            let msg = svc_schemas.resolve_state_key(state_key)?;
                            let steps = msg.resolve_path(field_path, &svc_schemas.pool)?;
                            Some((svc_schemas, steps))
                        });
                    last_lookup = Some((svc_name, state_key, field_path, r.clone()));
                    r
                }
            };

            if let Some((svc_schemas, steps)) = resolved
                && let Some(val_str) = extract_str(bytes, &steps, &svc_schemas.pool)
            {
                builder.append_value(val_str.as_ref());
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    };

    create_udf(
        "proto_get_str",
        vec![
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            DataType::LargeBinary,
            DataType::LargeUtf8,
        ],
        DataType::LargeUtf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

fn make_proto_get_int_udf<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        if args.len() != 4 {
            return Err(DataFusionError::Execution(
                "proto_get_int expects 4 arguments: (service_name, state_key, value, field_path)"
                    .to_string(),
            ));
        }
        let num_rows = batch_row_count(args);
        let a0 = to_array_opt(&args[0]);
        let a1 = to_array_opt(&args[1]);
        let a2 = to_array_opt(&args[2]);
        let a3 = to_array_opt(&args[3]);

        let mut builder = Int64Builder::with_capacity(num_rows);

        if let (Some(svc_name), Some(state_key), Some(field_path), Some(val_arr)) = (
            uniform_batch_str(&args[0], &a0),
            uniform_batch_str(&args[1], &a1),
            scalar_str(&args[3]),
            a2.as_ref()
                .and_then(|a| a.as_any().downcast_ref::<LargeBinaryArray>()),
        ) {
            let resolved = registry
                .resolve_service_schemas(svc_name)
                .and_then(|svc_schemas| {
                    let msg = svc_schemas.resolve_state_key(state_key)?;
                    msg.resolve_path(field_path, &svc_schemas.pool)
                });
            if let Some(steps) = resolved {
                let steps_slice = steps.as_slice();
                if val_arr.null_count() == 0 {
                    for i in 0..num_rows {
                        if let Some(val_int) = extract_i64(val_arr.value(i), steps_slice) {
                            builder.append_value(val_int);
                        } else {
                            builder.append_null();
                        }
                    }
                } else {
                    for i in 0..num_rows {
                        if val_arr.is_null(i) {
                            builder.append_null();
                        } else if let Some(val_int) = extract_i64(val_arr.value(i), steps_slice) {
                            builder.append_value(val_int);
                        } else {
                            builder.append_null();
                        }
                    }
                }
            } else {
                for _ in 0..num_rows {
                    builder.append_null();
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        }

        let mut last_lookup: Option<(&str, &str, &str, ResolvedSteps)> = None;

        for i in 0..num_rows {
            let (Some(svc_name), Some(state_key), Some(bytes), Some(field_path)) = (
                extract_str_arg(&args[0], i, &a0),
                extract_str_arg(&args[1], i, &a1),
                extract_bytes_arg(&args[2], i, &a2),
                extract_str_arg(&args[3], i, &a3),
            ) else {
                builder.append_null();
                continue;
            };

            let resolved = match &last_lookup {
                Some((s, k, p, cached))
                    if *s == svc_name && *k == state_key && *p == field_path =>
                {
                    cached.clone()
                }
                _ => {
                    let r = registry
                        .resolve_service_schemas(svc_name)
                        .and_then(|svc_schemas| {
                            let msg = svc_schemas.resolve_state_key(state_key)?;
                            msg.resolve_path(field_path, &svc_schemas.pool)
                        });
                    last_lookup = Some((svc_name, state_key, field_path, r.clone()));
                    r
                }
            };

            if let Some(steps) = resolved
                && let Some(val_int) = extract_i64(bytes, &steps)
            {
                builder.append_value(val_int);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    };

    create_udf(
        "proto_get_int",
        vec![
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            DataType::LargeBinary,
            DataType::LargeUtf8,
        ],
        DataType::Int64,
        Volatility::Stable,
        Arc::new(func),
    )
}

fn make_proto_get_float_udf<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        if args.len() != 4 {
            return Err(DataFusionError::Execution(
                "proto_get_float expects 4 arguments: (service_name, state_key, value, field_path)"
                    .to_string(),
            ));
        }
        let num_rows = batch_row_count(args);
        let a0 = to_array_opt(&args[0]);
        let a1 = to_array_opt(&args[1]);
        let a2 = to_array_opt(&args[2]);
        let a3 = to_array_opt(&args[3]);

        let mut builder = Float64Builder::with_capacity(num_rows);
        let mut last_lookup: Option<(&str, &str, &str, ResolvedSteps)> = None;

        for i in 0..num_rows {
            let (Some(svc_name), Some(state_key), Some(bytes), Some(field_path)) = (
                extract_str_arg(&args[0], i, &a0),
                extract_str_arg(&args[1], i, &a1),
                extract_bytes_arg(&args[2], i, &a2),
                extract_str_arg(&args[3], i, &a3),
            ) else {
                builder.append_null();
                continue;
            };

            let resolved = match &last_lookup {
                Some((s, k, p, cached))
                    if *s == svc_name && *k == state_key && *p == field_path =>
                {
                    cached.clone()
                }
                _ => {
                    let r = registry
                        .resolve_service_schemas(svc_name)
                        .and_then(|svc_schemas| {
                            let msg = svc_schemas.resolve_state_key(state_key)?;
                            msg.resolve_path(field_path, &svc_schemas.pool)
                        });
                    last_lookup = Some((svc_name, state_key, field_path, r.clone()));
                    r
                }
            };

            if let Some(steps) = resolved
                && let Some(val_f64) = extract_f64(bytes, &steps)
            {
                builder.append_value(val_f64);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    };

    create_udf(
        "proto_get_float",
        vec![
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            DataType::LargeBinary,
            DataType::LargeUtf8,
        ],
        DataType::Float64,
        Volatility::Stable,
        Arc::new(func),
    )
}

fn make_proto_get_bool_udf<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        if args.len() != 4 {
            return Err(DataFusionError::Execution(
                "proto_get_bool expects 4 arguments: (service_name, state_key, value, field_path)"
                    .to_string(),
            ));
        }
        let num_rows = batch_row_count(args);
        let a0 = to_array_opt(&args[0]);
        let a1 = to_array_opt(&args[1]);
        let a2 = to_array_opt(&args[2]);
        let a3 = to_array_opt(&args[3]);

        let mut builder = BooleanBuilder::with_capacity(num_rows);
        let mut last_lookup: Option<(&str, &str, &str, ResolvedSteps)> = None;

        for i in 0..num_rows {
            let (Some(svc_name), Some(state_key), Some(bytes), Some(field_path)) = (
                extract_str_arg(&args[0], i, &a0),
                extract_str_arg(&args[1], i, &a1),
                extract_bytes_arg(&args[2], i, &a2),
                extract_str_arg(&args[3], i, &a3),
            ) else {
                builder.append_null();
                continue;
            };

            let resolved = match &last_lookup {
                Some((s, k, p, cached))
                    if *s == svc_name && *k == state_key && *p == field_path =>
                {
                    cached.clone()
                }
                _ => {
                    let r = registry
                        .resolve_service_schemas(svc_name)
                        .and_then(|svc_schemas| {
                            let msg = svc_schemas.resolve_state_key(state_key)?;
                            msg.resolve_path(field_path, &svc_schemas.pool)
                        });
                    last_lookup = Some((svc_name, state_key, field_path, r.clone()));
                    r
                }
            };

            if let Some(steps) = resolved
                && let Some(val_b) = extract_bool(bytes, &steps)
            {
                builder.append_value(val_b);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    };

    create_udf(
        "proto_get_bool",
        vec![
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            DataType::LargeBinary,
            DataType::LargeUtf8,
        ],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

fn make_proto_to_json_udf<SMR: ServiceMetadataResolver + Send + Sync + Clone + 'static>(
    registry: DynamicProtoSchemaRegistry<SMR>,
) -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| -> DfResult<ColumnarValue> {
        if args.len() != 3 {
            return Err(DataFusionError::Execution(
                "proto_to_json expects 3 arguments: (service_name, state_key, value)".to_string(),
            ));
        }
        let num_rows = batch_row_count(args);
        let a0 = to_array_opt(&args[0]);
        let a1 = to_array_opt(&args[1]);
        let a2 = to_array_opt(&args[2]);

        let mut builder = LargeStringBuilder::with_capacity(num_rows, num_rows * 128);
        let mut last_lookup: Option<(&str, &str, ResolvedMsgSchema)> = None;

        for i in 0..num_rows {
            let (Some(svc_name), Some(state_key), Some(bytes)) = (
                extract_str_arg(&args[0], i, &a0),
                extract_str_arg(&args[1], i, &a1),
                extract_bytes_arg(&args[2], i, &a2),
            ) else {
                builder.append_null();
                continue;
            };

            let resolved = match &last_lookup {
                Some((s, k, cached)) if *s == svc_name && *k == state_key => cached.clone(),
                _ => {
                    let r = registry
                        .resolve_service_schemas(svc_name)
                        .and_then(|svc_schemas| {
                            let msg = svc_schemas.resolve_state_key(state_key)?.clone();
                            Some((svc_schemas, msg))
                        });
                    last_lookup = Some((svc_name, state_key, r.clone()));
                    r
                }
            };

            if let Some((svc_schemas, msg)) = resolved
                && let Some(json_val) = transcode_to_json(bytes, &msg, &svc_schemas.pool)
                && let Ok(json_str) = serde_json::to_string(&json_val)
            {
                builder.append_value(json_str);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    };

    create_udf(
        "proto_to_json",
        vec![
            DataType::LargeUtf8,
            DataType::LargeUtf8,
            DataType::LargeBinary,
        ],
        DataType::LargeUtf8,
        Volatility::Stable,
        Arc::new(func),
    )
}
