// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An [`OrderFunction`] of an ID column as a physical expression, so a scan can
//! declare the order its rows come out in.
//!
//! The expression is internal: no table has such a column and SQL can't name it.
//! It only appears in plans, as `base_entry_id(id@0)`. Its order is never the
//! order of the ID's text, so declaring it never lets a plan skip the sort an
//! `ORDER BY id` needs.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, FixedSizeBinaryBuilder, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

use restate_storage_query_api::ordering::OrderFunction;

/// `function(id)`: the function's value of each ID, in its byte form, whose byte
/// order is the value order.
#[derive(Debug, Clone, Eq)]
pub(crate) struct OrderKey {
    function: OrderFunction,
    id: Arc<dyn PhysicalExpr>,
}

impl OrderKey {
    pub(crate) fn new(function: OrderFunction, id: Arc<dyn PhysicalExpr>) -> Self {
        Self { function, id }
    }
}

impl PartialEq for OrderKey {
    fn eq(&self, other: &Self) -> bool {
        self.function == other.function && self.id.eq(&other.id)
    }
}

impl Hash for OrderKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.function.hash(state);
        self.id.hash(state);
    }
}

impl fmt::Display for OrderKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.function.name(), self.id)
    }
}

impl PhysicalExpr for OrderKey {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::FixedSizeBinary(self.function.width() as i32))
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.id.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let ids = self.id.evaluate(batch)?.into_array(batch.num_rows())?;
        let ids = cast(&ids, &DataType::Utf8)?;
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

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.id]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(self.function, children.swap_remove(0))))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.function.name())?;
        self.id.fmt_sql(f)?;
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{FixedSizeBinaryArray, LargeStringArray};
    use datafusion::arrow::datatypes::Field;
    use datafusion::physical_expr::expressions::Column;

    use restate_types::identifiers::{InvocationId, InvocationUuid};

    use super::*;

    /// Each row gets the function's bytes of its ID, NULL stays NULL, and text
    /// that isn't such an ID fails the evaluation.
    #[test]
    fn rows_get_their_function_bytes() {
        let id = InvocationId::from_parts(7, InvocationUuid::from_u128(1)).to_string();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::LargeUtf8,
            true,
        )]));
        let batch = |ids: Vec<Option<&str>>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(LargeStringArray::from(ids))],
            )
            .unwrap()
        };
        for function in [OrderFunction::InvocationId, OrderFunction::BaseEntryId] {
            let key = OrderKey::new(function, Arc::new(Column::new("id", 0)));
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
