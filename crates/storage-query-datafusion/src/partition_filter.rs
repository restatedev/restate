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
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, col};
use restate_types::identifiers::partitioner::HashPartitioner;
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

pub trait PartitionKeyExtractor: Send + Sync + 'static + Debug {
    fn try_extract(&self, filters: &[Expr]) -> anyhow::Result<Option<PartitionKey>>;
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
    fn try_extract(&self, filters: &[Expr]) -> anyhow::Result<Option<PartitionKey>> {
        for extractor in &self.extractors {
            if let Some(partition_key) = extractor.try_extract(filters)? {
                return Ok(Some(partition_key));
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
    fn try_extract(&self, filters: &[Expr]) -> anyhow::Result<Option<PartitionKey>> {
        for filter in filters {
            if let Expr::BinaryExpr(BinaryExpr { op, left, right }) = filter
                && *op == Operator::Eq
                && **left == self.column
                && let Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _) = &**right
            {
                let f = &self.extractor;
                let pk = f(value)?;
                return Ok(Some(pk));
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
    fn try_extract(&self, filters: &[Expr]) -> anyhow::Result<Option<PartitionKey>> {
        for filter in filters {
            if let Expr::BinaryExpr(BinaryExpr { op, left, right }) = filter
                && *op == Operator::Eq
                && **left == self.0
                && let Expr::Literal(ScalarValue::UInt64(Some(value)), _) = &**right
            {
                return Ok(Some(*value as PartitionKey));
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

        let got_key = extractor
            .try_extract(&[col("service_key").eq(utf8_lit("key-1"))])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(expected_key, got_key);
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

        let got_key = extractor
            .try_extract(&[col("id").eq(utf8_lit(column_value))])
            .expect("extract")
            .expect("to find a value");

        assert_eq!(expected_key, got_key);
    }

    fn utf8_lit(value: impl Into<String>) -> Expr {
        Expr::Literal(ScalarValue::LargeUtf8(Some(value.into())), None)
    }
}
