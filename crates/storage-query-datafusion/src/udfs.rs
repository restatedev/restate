// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, FixedSizeBinaryArray, UInt64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use restate_types::identifiers::{InvocationId, InvocationUuid, ResourceId};

/// UDF that reconstructs an invocation ID from partition_key and uuid.
/// Usage: `restate_invocation_id(partition_key, uuid)`
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct RestateInvocationIdUdf {
    signature: Signature,
}

impl RestateInvocationIdUdf {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::UInt64, DataType::FixedSizeBinary(16)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RestateInvocationIdUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "restate_invocation_id"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::LargeUtf8)
    }

    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        use datafusion::arrow::array::LargeStringBuilder;
        use std::fmt::Write;

        let args = args.args;

        let partition_keys = match &args[0] {
            ColumnarValue::Array(arr) => {
                arr.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                    DataFusionError::Execution(
                        "restate_invocation_id: partition_key must be UInt64".to_string(),
                    )
                })?
            }
            ColumnarValue::Scalar(s) => {
                return Err(DataFusionError::Execution(format!(
                    "restate_invocation_id: expected array for partition_key, got scalar {:?}",
                    s
                )));
            }
        };

        let uuids = match &args[1] {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "restate_invocation_id: uuid must be FixedSizeBinary(16)".to_string(),
                    )
                })?,
            ColumnarValue::Scalar(s) => {
                return Err(DataFusionError::Execution(format!(
                    "restate_invocation_id: expected array for uuid, got scalar {:?}",
                    s
                )));
            }
        };

        let num_rows = partition_keys.len();
        let mut builder =
            LargeStringBuilder::with_capacity(num_rows, num_rows * InvocationId::str_encoded_len());

        for i in 0..num_rows {
            if partition_keys.is_null(i) || uuids.is_null(i) {
                builder.append_null();
            } else {
                let partition_key = partition_keys.value(i);
                let uuid_bytes: [u8; 16] = uuids.value(i).try_into().map_err(|_| {
                    DataFusionError::Internal(
                        "restate_invocation_id: uuid must be exactly 16 bytes".to_string(),
                    )
                })?;
                let invocation_uuid = InvocationUuid::from_bytes(uuid_bytes);
                let invocation_id = InvocationId::from_parts(partition_key, invocation_uuid);
                let _ = write!(&mut builder, "{invocation_id}");
                builder.append_value("");
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}
