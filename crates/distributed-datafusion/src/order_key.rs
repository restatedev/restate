// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An [`OrderFunction`] of an ID column as a physical expression, so a read can
//! declare the order its rows come out in and a JOIN can merge on it.
//!
//! Each function is a scalar function (UDF) under its name, `base_entry_id(id@0)`
//! in plans, so plans carry it to workers by name like any function call. Only
//! workers register the functions ([`register_on_worker`]): SQL can't name them,
//! and no table has such a column. Their order is never the order of the ID's
//! text, so declaring it never lets a plan skip the sort an `ORDER BY id` needs.
//!
//! Expressions compare equal when their function, arguments and config do. So the
//! coordinator builds every one of them with one shared config ([`order_key`]):
//! the order a read declares then matches the key a JOIN asks for.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, FixedSizeBinaryBuilder, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Result, exec_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use strum::VariantArray;

use restate_storage_query_api::ordering::OrderFunction;

/// `function(id)`: the function's value of each ID, in its byte form, whose byte
/// order is the value order.
pub(crate) fn order_key(
    function: OrderFunction,
    id: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    static CONFIG: LazyLock<Arc<ConfigOptions>> = LazyLock::new(Arc::default);
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        udf(function),
        vec![id],
        schema,
        Arc::clone(&CONFIG),
    )?))
}

/// Registers every ordering function, for workers to run the plans that use them.
pub(crate) fn register_on_worker(registry: &mut dyn FunctionRegistry) -> Result<()> {
    for &function in OrderFunction::VARIANTS {
        registry.register_udf(udf(function))?;
    }
    Ok(())
}

fn udf(function: OrderFunction) -> Arc<ScalarUDF> {
    static UDFS: LazyLock<Vec<Arc<ScalarUDF>>> = LazyLock::new(|| {
        OrderFunction::VARIANTS
            .iter()
            .map(|&function| {
                Arc::new(ScalarUDF::new_from_impl(OrderUdf {
                    function,
                    signature: Signature::string(1, Volatility::Immutable),
                }))
            })
            .collect()
    });
    let at = OrderFunction::VARIANTS
        .iter()
        .position(|&known| known == function)
        .expect("every function is a variant");
    Arc::clone(&UDFS[at])
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct OrderUdf {
    function: OrderFunction,
    signature: Signature,
}

impl ScalarUDFImpl for OrderUdf {
    fn name(&self) -> &str {
        self.function.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::FixedSizeBinary(self.function.width() as i32))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [ids] = args.args.as_slice() else {
            return exec_err!("{} takes one ID", self.function.name());
        };
        let ids = cast(&ids.to_array(args.number_rows)?, &DataType::Utf8)?;
        let ids = ids
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8 yields a StringArray");
        let mut keys =
            FixedSizeBinaryBuilder::with_capacity(ids.len(), self.function.width() as i32);
        for id in ids {
            match id {
                None => keys.append_null(),
                Some(text) => match self.function.encode(text, |value| keys.append_value(value)) {
                    Ok(appended) => appended?,
                    Err(err) => {
                        return exec_err!("no {} for {text:?}: {err}", self.function.name());
                    }
                },
            }
        }
        Ok(ColumnarValue::Array(Arc::new(keys.finish())))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{FixedSizeBinaryArray, LargeStringArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;

    use restate_types::identifiers::{InvocationId, InvocationUuid};

    use super::*;

    /// Each row gets the function's bytes of its ID, NULL stays NULL, text that
    /// isn't such an ID fails the evaluation, and keys built apart compare equal.
    #[test]
    fn rows_get_their_function_bytes() {
        let id = InvocationId::from_parts(7, InvocationUuid::from_u128(1)).to_string();
        let schema = Schema::new(vec![Field::new("id", DataType::LargeUtf8, true)]);
        let batch = |ids: Vec<Option<&str>>| {
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new(LargeStringArray::from(ids))],
            )
            .unwrap()
        };
        for function in [OrderFunction::InvocationId, OrderFunction::BaseEntryId] {
            let key = order_key(function, Arc::new(Column::new("id", 0)), &schema).unwrap();
            let again = order_key(function, Arc::new(Column::new("id", 0)), &schema).unwrap();
            assert!(key.eq(&again));
            assert_eq!(key.to_string(), format!("{}(id@0)", function.name()));
            let keys = key
                .evaluate(&batch(vec![Some(&id), None]))
                .unwrap()
                .into_array(2)
                .unwrap();
            let keys = keys
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(keys.value(0), function.encode(&id, <[u8]>::to_vec).unwrap());
            assert!(keys.is_null(1));
            assert!(key.evaluate(&batch(vec![Some("not an id")])).is_err());
        }
    }
}
