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

use datafusion::arrow::array::{Array, ArrayRef, LargeStringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::{as_generic_binary_array, as_generic_string_array};
use datafusion::error::DataFusionError;
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion_expr::{create_udf, Volatility};

use restate_schema_api::key::RestateKeyConverter;

use crate::context::QueryContext;

pub(crate) fn register_udf(
    ctx: &QueryContext,
    resolver: impl RestateKeyConverter + Sync + Send + 'static,
) {
    ctx.as_ref().register_udf(create_udf(
        "key_to_json",
        vec![DataType::LargeUtf8, DataType::LargeBinary],
        Arc::new(DataType::LargeUtf8),
        Volatility::Immutable,
        make_scalar_function(move |args| {
            match (args[0].data_type(), args[1].data_type()) {
                (DataType::LargeUtf8, DataType::LargeBinary) => (),
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data types {other:?} for function key_to_json."
                    )));
                }
            };
            let service_array = as_generic_string_array::<i64>(&args[0])?;
            let key_array = as_generic_binary_array::<i64>(&args[1])?;

            let result = service_array
                .iter()
                .zip(key_array.iter())
                .map(|(service, key)| match (service, key) {
                    (Some(service), Some(key)) => match resolver.key_to_json(service, key) {
                        Ok(value) => Some(format!("{}", value)),
                        Err(_) => None,
                    },
                    _ => None,
                })
                .collect::<LargeStringArray>();

            Ok(Arc::new(result) as ArrayRef)
        }),
    ))
}
