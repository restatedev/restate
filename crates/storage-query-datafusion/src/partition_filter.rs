// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, InListExpr, Literal};

use restate_types::identifiers::partitioner::HashPartitioner;
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};

pub trait PartitionKeyExtractor: Send + Sync + 'static + Debug {
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>>;
}

#[derive(Debug)]
pub struct FirstMatchingPartitionKeyExtractor {
    extractors: Vec<Box<dyn PartitionKeyExtractor>>,
}

impl Default for FirstMatchingPartitionKeyExtractor {
    fn default() -> Self {
        let extractors = vec![Box::new(MatchingColumnExtractor::new(
            "partition_key",
            |value: &ScalarValue| match value {
                ScalarValue::UInt64(Some(v)) => Ok(*v),
                _ => anyhow::bail!("expected UInt64 partition key"),
            },
        )) as Box<dyn PartitionKeyExtractor>];
        Self { extractors }
    }
}

impl FirstMatchingPartitionKeyExtractor {
    pub fn with_service_key(self, column_name: impl Into<String>) -> Self {
        let e = MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected string service key")?
                .context("unexpected null service key")?;
            Ok(HashPartitioner::compute_partition_key(value))
        });
        self.append(e)
    }

    pub fn with_invocation_id(self, column_name: impl Into<String>) -> Self {
        let e = MatchingColumnExtractor::new(column_name, |value: &ScalarValue| {
            let value = value
                .try_as_str()
                .context("expected string invocation id")?
                .context("unexpected null invocation id")?;
            let invocation_id = InvocationId::from_str(value).context("non valid invocation id")?;
            Ok(invocation_id.partition_key())
        });
        self.append(e)
    }

    pub fn append(mut self, extractor: impl PartitionKeyExtractor) -> Self {
        self.extractors.push(Box::new(extractor));
        self
    }
}

impl PartitionKeyExtractor for FirstMatchingPartitionKeyExtractor {
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        for extractor in &self.extractors {
            if let Some(partition_keys) = extractor.try_extract(filters)? {
                return Ok(Some(partition_keys));
            }
        }

        Ok(None)
    }
}

pub(crate) struct MatchingColumnExtractor<F> {
    column_name: String,
    extractor: F,
}

impl<F> Debug for MatchingColumnExtractor<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "MatchingColumnExtractor({:?})",
            self.column_name
        ))
    }
}

impl<F> MatchingColumnExtractor<F> {
    pub(crate) fn new(column_name: impl Into<String>, extractor: F) -> Self {
        Self {
            column_name: column_name.into(),
            extractor,
        }
    }
}

impl<F> PartitionKeyExtractor for MatchingColumnExtractor<F>
where
    F: Fn(&ScalarValue) -> anyhow::Result<PartitionKey> + Send + Sync + 'static,
{
    /// Find an expression in the form of `$column_name = <literal>`.
    /// Then use the provided extractor to convert the literal value to a partition_key.
    fn try_extract(
        &self,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        for filter in filters {
            let Some(inlist) = InList::parse(filter, 5) else {
                continue;
            };

            if inlist.col.name() != self.column_name {
                continue;
            }

            let mut list_keys = BTreeSet::new();

            for value in &inlist.list {
                let pk = (self.extractor)(value)?;
                list_keys.insert(pk);
            }

            return Ok(Some(list_keys));
        }

        Ok(None)
    }
}

/// A normalized representation of predicates that compare a column to literal values.
/// Handles `col = lit`, `col IN (lit, ...)`, and `col = lit OR col = lit ...` patterns.
struct InList<'a> {
    col: &'a Column,
    list: HashSet<&'a ScalarValue>,
    negated: bool,
}

impl<'a> InList<'a> {
    fn parse(predicate: &'a Arc<dyn PhysicalExpr>, depth_limit: usize) -> Option<Self> {
        if depth_limit <= 1 {
            return None;
        }

        // Handle IN list: col IN ('a', 'b', ...)
        if let Some(in_list) = predicate.as_any().downcast_ref::<InListExpr>() {
            let col = in_list.expr().as_any().downcast_ref::<Column>()?;

            let mut list = HashSet::with_capacity(in_list.len());
            for lit in in_list.list() {
                let lit = lit.as_any().downcast_ref::<Literal>()?;
                list.insert(lit.value());
            }

            return Some(InList {
                col,
                list,
                negated: in_list.negated(),
            });
        }

        let binary = predicate.as_any().downcast_ref::<BinaryExpr>()?;

        match binary.op() {
            // Handle simple equality: col = 'a'
            Operator::Eq => {
                let (col, lit) = extract_column_literal(binary.left(), binary.right())
                    .or_else(|| extract_column_literal(binary.right(), binary.left()))?;

                Some(InList {
                    col,
                    list: HashSet::from_iter([lit.value()]),
                    negated: false,
                })
            }
            // Handle OR: col = 'a' OR col = 'b'
            Operator::Or => {
                let mut left = Self::parse(binary.left(), depth_limit - 1)?;
                let right = Self::parse(binary.right(), depth_limit - 1)?;

                if left.col.name() == right.col.name() && !left.negated && !right.negated {
                    left.list.extend(right.list);
                    Some(InList {
                        col: left.col,
                        list: left.list,
                        negated: false,
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

fn extract_column_literal<'a>(
    column: &'a Arc<dyn PhysicalExpr>,
    literal: &'a Arc<dyn PhysicalExpr>,
) -> Option<(&'a Column, &'a Literal)> {
    let col = column.as_any().downcast_ref::<Column>()?;
    let lit = literal.as_any().downcast_ref::<Literal>()?;
    Some((col, lit))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::ScalarValue;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column, InListExpr, Literal};

    use restate_types::identifiers::{InvocationId, ServiceId, WithPartitionKey};
    use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType};

    use crate::partition_filter::{FirstMatchingPartitionKeyExtractor, PartitionKeyExtractor};

    fn col(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    fn utf8_lit(value: impl Into<String>) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::LargeUtf8(Some(value.into()))))
    }

    fn eq(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            left,
            datafusion::logical_expr::Operator::Eq,
            right,
        ))
    }

    fn or(left: Arc<dyn PhysicalExpr>, right: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            left,
            datafusion::logical_expr::Operator::Or,
            right,
        ))
    }

    fn in_list(col_name: &str, list: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![Field::new(col_name, DataType::LargeUtf8, true)]);
        Arc::new(InListExpr::try_new(col(col_name), list, false, &schema).expect("valid in-list"))
    }

    #[test]
    fn test_service_key() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id = ServiceId::new("greeter", "key-1");
        let expected_key = service_id.partition_key();

        let got_keys = extractor
            .try_extract(&[eq(col("service_key"), utf8_lit("key-1"))])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(1, got_keys.len());
        assert_eq!(expected_key, got_keys.into_iter().next().unwrap());
    }

    #[test]
    fn test_multiple_service_keys() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id_1 = ServiceId::new("greeter", "key-1");
        let service_id_2 = ServiceId::new("greeter", "key-2");
        let expected_key_1 = service_id_1.partition_key();
        let expected_key_2 = service_id_2.partition_key();

        let got_keys = extractor
            .try_extract(&[in_list(
                "service_key",
                vec![utf8_lit("key-1"), utf8_lit("key-2")],
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(2, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    #[test]
    fn test_multiple_service_keys_ored() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id_1 = ServiceId::new("greeter", "key-1");
        let service_id_2 = ServiceId::new("greeter", "key-2");
        let expected_key_1 = service_id_1.partition_key();
        let expected_key_2 = service_id_2.partition_key();

        let got_keys = extractor
            .try_extract(&[or(
                eq(col("service_key"), utf8_lit("key-1")),
                eq(col("service_key"), utf8_lit("key-2")),
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(2, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    #[test]
    fn test_multiple_service_keys_nested_or() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id_1 = ServiceId::new("greeter", "key-1");
        let service_id_2 = ServiceId::new("greeter", "key-2");
        let service_id_3 = ServiceId::new("greeter", "key-3");
        let service_id_4 = ServiceId::new("greeter", "key-4");
        let expected_key_1 = service_id_1.partition_key();
        let expected_key_2 = service_id_2.partition_key();
        let expected_key_3 = service_id_3.partition_key();
        let expected_key_4 = service_id_4.partition_key();

        let got_keys = extractor
            .try_extract(&[or(
                or(
                    eq(col("service_key"), utf8_lit("key-1")),
                    eq(col("service_key"), utf8_lit("key-2")),
                ),
                or(
                    eq(col("service_key"), utf8_lit("key-3")),
                    eq(col("service_key"), utf8_lit("key-4")),
                ),
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(4, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_4, got_keys.next().unwrap());
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_3, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    #[test]
    fn test_multiple_service_keys_too_deep_nesting() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let got_keys = extractor
            .try_extract(&[or(
                or(
                    eq(col("service_key"), utf8_lit("key-1")),
                    or(
                        eq(col("service_key"), utf8_lit("key-2")),
                        or(
                            eq(col("service_key"), utf8_lit("key-3")),
                            eq(col("service_key"), utf8_lit("key-4")),
                        ),
                    ),
                ),
                eq(col("service_key"), utf8_lit("key-7")),
            )])
            .expect("extract");

        assert_eq!(None, got_keys);
    }

    #[test]
    fn test_invocation_id() {
        let extractor = FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id");

        let invocation_target = InvocationTarget::virtual_object(
            "counter",
            "key-2",
            "count",
            VirtualObjectHandlerType::Exclusive,
        );
        let invocation_id = InvocationId::generate(&invocation_target, None);
        let expected_key = invocation_id.partition_key();
        let column_value = invocation_id.to_string();

        let got_keys = extractor
            .try_extract(&[eq(col("id"), utf8_lit(column_value))])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(1, got_keys.len());
        assert_eq!(expected_key, got_keys.into_iter().next().unwrap());
    }

    #[test]
    fn test_multiple_invocation_ids() {
        let extractor = FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id");

        let invocation_target_1 = InvocationTarget::virtual_object(
            "counter",
            "key-1",
            "add",
            VirtualObjectHandlerType::Exclusive,
        );

        let invocation_target_2 = InvocationTarget::virtual_object(
            "counter",
            "key-2",
            "count",
            VirtualObjectHandlerType::Exclusive,
        );

        let invocation_id_1 = InvocationId::generate(&invocation_target_1, None);
        let invocation_id_2 = InvocationId::generate(&invocation_target_2, None);
        let expected_key_1 = invocation_id_1.partition_key();
        let expected_key_2 = invocation_id_2.partition_key();

        let got_keys = extractor
            .try_extract(&[in_list(
                "id",
                vec![
                    utf8_lit(invocation_id_1.to_string()),
                    utf8_lit(invocation_id_2.to_string()),
                ],
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(2, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    #[test]
    fn test_invalid_in_list() {
        let extractor = FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id");

        let invocation_target = InvocationTarget::virtual_object(
            "counter",
            "key-1",
            "add",
            VirtualObjectHandlerType::Exclusive,
        );

        let invocation_id = InvocationId::generate(&invocation_target, None);

        // An OR where one side has a non-literal (column) should not be extractable
        let got_keys = extractor
            .try_extract(&[or(
                eq(col("id"), utf8_lit(invocation_id.to_string())),
                eq(col("id"), col("some_other_col")),
            )])
            .expect("extract");

        assert_eq!(None, got_keys);
    }
}
