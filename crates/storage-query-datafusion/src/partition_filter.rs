// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, col};
use restate_types::identifiers::partitioner::HashPartitioner;
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

pub trait PartitionKeyExtractor: Send + Sync + 'static + Debug {
    fn try_extract(&self, filters: &[Expr]) -> anyhow::Result<Option<BTreeSet<PartitionKey>>>;
}

#[derive(Debug)]
pub struct FirstMatchingPartitionKeyExtractor {
    extractors: Vec<Box<dyn PartitionKeyExtractor>>,
}

impl Default for FirstMatchingPartitionKeyExtractor {
    fn default() -> Self {
        let extractors =
            vec![Box::new(IdentityPartitionKeyExtractor::new()) as Box<dyn PartitionKeyExtractor>];
        Self { extractors }
    }
}

impl FirstMatchingPartitionKeyExtractor {
    pub fn with_service_key(self, column_name: impl Into<String>) -> Self {
        let e = MatchingColumnExtractor::new(column_name, |column_value: &str| {
            let key = HashPartitioner::compute_partition_key(column_value);
            Ok(key)
        });
        self.append(e)
    }

    pub fn with_invocation_id(self, column_name: impl Into<String>) -> Self {
        let e = MatchingColumnExtractor::new(column_name, |column_value: &str| {
            let invocation_id =
                InvocationId::from_str(column_value).context("non valid invocation id")?;
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
    fn try_extract(&self, filters: &[Expr]) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        for extractor in &self.extractors {
            if let Some(partition_keys) = extractor.try_extract(filters)? {
                return Ok(Some(partition_keys));
            }
        }

        Ok(None)
    }
}

pub(crate) struct MatchingColumnExtractor<F> {
    column: Expr,
    extractor: F,
}

impl<F> Debug for MatchingColumnExtractor<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("MatchingColumnExtractor{:?}", self.column))
    }
}

impl<F> MatchingColumnExtractor<F> {
    pub(crate) fn new(column_name: impl Into<String>, extractor: F) -> Self {
        let column = col(column_name.into());
        Self { column, extractor }
    }
}

impl<F> PartitionKeyExtractor for MatchingColumnExtractor<F>
where
    F: Fn(&str) -> anyhow::Result<PartitionKey> + Send + Sync + 'static,
{
    /// find an expression in the form of `$column_name = "..."`.
    /// Then use the provided extractor to convert the textual value to a
    /// partition_key
    fn try_extract(&self, filters: &[Expr]) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        'filters: for filter in filters {
            match filter {
                Expr::BinaryExpr(BinaryExpr {
                    op: Operator::Eq,
                    left,
                    right,
                }) if **left == self.column => {
                    if let Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) = &**right {
                        let f = &self.extractor;
                        let pk = f(value)?;
                        return Ok(Some(BTreeSet::from([pk])));
                    }
                }
                Expr::InList(InList {
                    expr,
                    list,
                    negated: false,
                }) if **expr == self.column => {
                    let mut list_keys = BTreeSet::new();

                    for item in list {
                        if let Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) = item {
                            let f = &self.extractor;
                            let pk = f(value)?;
                            list_keys.insert(pk);
                        } else {
                            // items in the list are ORed. If we can't parse one, we can't apply this list
                            continue 'filters;
                        }
                    }

                    return Ok(Some(list_keys));
                }
                _ => {}
            }
        }

        Ok(None)
    }
}

#[derive(Debug)]
struct IdentityPartitionKeyExtractor(Expr);

impl IdentityPartitionKeyExtractor {
    fn new() -> Self {
        Self(col("partition_key"))
    }
}

impl PartitionKeyExtractor for IdentityPartitionKeyExtractor {
    fn try_extract(&self, filters: &[Expr]) -> Result<Option<BTreeSet<u64>>, anyhow::Error> {
        'filters: for filter in filters {
            match filter {
                Expr::BinaryExpr(BinaryExpr {
                    op: Operator::Eq,
                    left,
                    right,
                }) if **left == self.0 => {
                    if let Expr::Literal(ScalarValue::UInt64(Some(value)), _) = &**right {
                        return Ok(Some(BTreeSet::from([*value as PartitionKey])));
                    }
                }
                Expr::InList(InList {
                    expr,
                    list,
                    negated: false,
                }) if **expr == self.0 => {
                    let mut list_keys = BTreeSet::new();

                    for item in list {
                        if let Expr::Literal(ScalarValue::UInt64(Some(value)), _) = item {
                            list_keys.insert(*value as PartitionKey);
                        } else {
                            // items in the list are ORed. If we can't parse one, we can't apply this list
                            continue 'filters;
                        }
                    }

                    return Ok(Some(list_keys));
                }
                _ => {}
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::partition_filter::{FirstMatchingPartitionKeyExtractor, PartitionKeyExtractor};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col};
    use restate_types::identifiers::{InvocationId, ServiceId, WithPartitionKey};
    use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType};

    #[test]
    fn test_service_key() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id = ServiceId::new("greeter", "key-1");
        let expected_key = service_id.partition_key();

        let got_keys = extractor
            .try_extract(&[col("service_key").eq(utf8_lit("key-1"))])
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
            .try_extract(&[
                col("service_key").in_list(vec![utf8_lit("key-1"), utf8_lit("key-2")], false)
            ])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(2, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
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
            .try_extract(&[col("id").eq(utf8_lit(column_value))])
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
            .try_extract(&[col("id").in_list(
                vec![
                    utf8_lit(invocation_id_1.to_string()),
                    utf8_lit(invocation_id_2.to_string()),
                ],
                false,
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

        let got_keys = extractor
            .try_extract(&[col("id").in_list(
                vec![utf8_lit(invocation_id.to_string()), col("some_other_col")],
                false,
            )])
            .expect("extract");

        assert_eq!(None, got_keys);
    }

    fn utf8_lit(value: impl Into<String>) -> Expr {
        Expr::Literal(ScalarValue::LargeUtf8(Some(value.into())), None)
    }
}
