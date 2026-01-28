// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, ScalarUDF, col};

use restate_types::identifiers::partitioner::HashPartitioner;
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};

use crate::udfs::RestateInvocationIdUdf;

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
        let e = MatchingExpressionExtractor::new(col(column_name.into()), |column_value: &str| {
            let key = HashPartitioner::compute_partition_key(column_value);
            Ok(key)
        });
        self.append(e)
    }

    pub fn with_invocation_id(self, column_name: impl Into<String>) -> Self {
        let e = MatchingExpressionExtractor::new(col(column_name.into()), |column_value: &str| {
            let invocation_id =
                InvocationId::from_str(column_value).context("non valid invocation id")?;
            Ok(invocation_id.partition_key())
        });
        self.append(e)
    }

    /// Accept filters on a `restate_invocation_id(partition_key, uuid)` UDF call,
    /// such as those pushed down from views that define `id` as a computed column.
    pub fn with_invocation_id_udf(self, uuid_col: impl Into<String>) -> Self {
        let udf_expr = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(RestateInvocationIdUdf::new())),
            args: vec![col("partition_key"), col(uuid_col.into())],
        });
        let e = MatchingExpressionExtractor::new(udf_expr, |column_value: &str| {
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

pub(crate) struct MatchingExpressionExtractor<F> {
    expr: Expr,
    extractor: F,
}

impl<F> Debug for MatchingExpressionExtractor<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("MatchingExpressionExtractor{:?}", self.expr))
    }
}

impl<F> MatchingExpressionExtractor<F> {
    pub(crate) fn new(column: Expr, extractor: F) -> Self {
        Self {
            expr: column,
            extractor,
        }
    }
}

impl<F> PartitionKeyExtractor for MatchingExpressionExtractor<F>
where
    F: Fn(&str) -> anyhow::Result<PartitionKey> + Send + Sync + 'static,
{
    /// find an expression in the form of `$column_name = "..."`.
    /// Then use the provided extractor to convert the textual value to a
    /// partition_key
    fn try_extract(&self, filters: &[Expr]) -> anyhow::Result<Option<BTreeSet<PartitionKey>>> {
        'filters: for filter in filters {
            let Some(filter_as_inlist) = as_inlist(filter, 5) else {
                continue;
            };

            if *filter_as_inlist.expr != self.expr {
                continue;
            }

            let mut list_keys = BTreeSet::new();
            for item in &filter_as_inlist.list {
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
        Ok(None)
    }
}

/// Returns true if the expression is one that we support as the target of a
/// partition key filter: column references, or scalar UDF calls over columns.
fn is_filterable_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::ScalarFunction(f) => f.args.iter().all(|arg| matches!(arg, Expr::Column(_))),
        _ => false,
    }
}

/// Try to convert an expression to an in-list expression, recursively handling OR if needed
/// Adapted from datafusion functions `as_inlist` and `are_inlist_and_eq`
fn as_inlist(expr: &Expr, depth_limit: usize) -> Option<Cow<'_, InList>> {
    if depth_limit <= 1 {
        return None;
    }
    match expr {
        Expr::InList(inlist) => Some(Cow::Borrowed(inlist)),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => match (left.as_ref(), right.as_ref()) {
            (expr, Expr::Literal(_, _)) if is_filterable_expr(expr) => Some(Cow::Owned(InList {
                expr: left.clone(),
                list: vec![*right.clone()],
                negated: false,
            })),
            (Expr::Literal(_, _), expr) if is_filterable_expr(expr) => Some(Cow::Owned(InList {
                expr: right.clone(),
                list: vec![*left.clone()],
                negated: false,
            })),
            _ => None,
        },
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            let left_as_inlist = as_inlist(left, depth_limit - 1)?;
            let right_as_inlist = as_inlist(right, depth_limit - 1)?;

            if is_filterable_expr(&left_as_inlist.expr)
                && is_filterable_expr(&right_as_inlist.expr)
                && left_as_inlist.expr == right_as_inlist.expr
                && !left_as_inlist.negated
                && !right_as_inlist.negated
            {
                let mut seen: HashSet<Expr> = HashSet::new();
                let list = left_as_inlist
                    .list
                    .iter()
                    .cloned()
                    .chain(right_as_inlist.list.iter().cloned())
                    .filter(|e| seen.insert(e.to_owned()))
                    .collect::<Vec<_>>();

                Some(Cow::Owned(InList {
                    expr: left_as_inlist.expr.clone(),
                    list,
                    negated: false,
                }))
            } else {
                None
            }
        }
        _ => None,
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
            let Some(filter_as_inlist) = as_inlist(filter, 5) else {
                continue;
            };

            if *filter_as_inlist.expr != self.0 {
                continue;
            }

            let mut list_keys = BTreeSet::new();

            for item in &filter_as_inlist.list {
                if let Expr::Literal(ScalarValue::UInt64(Some(value)), _) = item {
                    list_keys.insert(*value as PartitionKey);
                } else {
                    // items in the list are ORed. If we can't parse one, we can't apply this list
                    continue 'filters;
                }
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, ScalarUDF, col, or};

    use restate_types::identifiers::{InvocationId, ServiceId, WithPartitionKey};
    use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType};

    use crate::partition_filter::{FirstMatchingPartitionKeyExtractor, PartitionKeyExtractor};
    use crate::udfs::RestateInvocationIdUdf;

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
    fn test_multiple_service_keys_ored() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_service_key("service_key");

        let service_id_1 = ServiceId::new("greeter", "key-1");
        let service_id_2 = ServiceId::new("greeter", "key-2");
        let expected_key_1 = service_id_1.partition_key();
        let expected_key_2 = service_id_2.partition_key();

        let got_keys = extractor
            .try_extract(&[or(
                col("service_key").eq(utf8_lit("key-1")),
                col("service_key").eq(utf8_lit("key-2")),
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
                    col("service_key").eq(utf8_lit("key-1")),
                    col("service_key").eq(utf8_lit("key-2")),
                ),
                or(
                    col("service_key").eq(utf8_lit("key-3")),
                    col("service_key").eq(utf8_lit("key-4")),
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
                    col("service_key").eq(utf8_lit("key-1")),
                    or(
                        col("service_key").eq(utf8_lit("key-2")),
                        or(
                            col("service_key").eq(utf8_lit("key-3")),
                            col("service_key").eq(utf8_lit("key-4")),
                        ),
                    ),
                ),
                col("service_key").eq(utf8_lit("key-7")),
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

    #[test]
    fn test_invocation_id_udf_eq() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_invocation_id_udf("uuid");

        let invocation_target = InvocationTarget::virtual_object(
            "counter",
            "key-2",
            "count",
            VirtualObjectHandlerType::Exclusive,
        );
        let invocation_id = InvocationId::generate(&invocation_target, None);
        let expected_key = invocation_id.partition_key();

        let udf_expr = udf_invocation_id_expr();
        let got_keys = extractor
            .try_extract(&[udf_expr.eq(utf8_lit(invocation_id.to_string()))])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(1, got_keys.len());
        assert_eq!(expected_key, got_keys.into_iter().next().unwrap());
    }

    #[test]
    fn test_invocation_id_udf_ored() {
        let extractor =
            FirstMatchingPartitionKeyExtractor::default().with_invocation_id_udf("uuid");

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

        let udf_expr = udf_invocation_id_expr();
        let got_keys = extractor
            .try_extract(&[or(
                udf_expr.clone().eq(utf8_lit(invocation_id_1.to_string())),
                udf_expr.eq(utf8_lit(invocation_id_2.to_string())),
            )])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(2, got_keys.len());
        let mut got_keys = got_keys.into_iter();
        assert_eq!(expected_key_1, got_keys.next().unwrap());
        assert_eq!(expected_key_2, got_keys.next().unwrap());
    }

    fn udf_invocation_id_expr() -> Expr {
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(RestateInvocationIdUdf::new())),
            args: vec![col("partition_key"), col("uuid")],
        })
    }

    fn utf8_lit(value: impl Into<String>) -> Expr {
        Expr::Literal(ScalarValue::LargeUtf8(Some(value.into())), None)
    }
}
