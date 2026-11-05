// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL helpers shared by the query engines.

use std::ops::ControlFlow;

use datafusion_sql::parser::Statement;
use datafusion_sql::sqlparser::ast::{
    ArrayElemTypeDef, DataType, Expr, FunctionArgumentClause, FunctionArguments, Ident,
    JsonTableColumn, LimitClause, Query, Statement as SqlStatement, TableAlias, TableFactor,
    TypedString, Value, ValueWithSpan, VisitMut, VisitorMut, XmlTableColumnOption,
};
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::tokenizer::{Token, Tokenizer};

use restate_util_string::{ReString, ToReString};

/// Formats a diagnostic copy of the parsed query, never the AST used for planning.
/// Literals (including placeholders) become `?`; comments are discarded by the parser.
/// Plain numbers in `LIMIT`, `OFFSET` and `FETCH` stay visible: they are row counts
/// that describe the query, not its data. Identifiers, aliases, and numeric type
/// parameters remain visible. This is not executable SQL.
/// Statements outside the query subset are omitted: some store values as bare strings
/// that sqlparser's value visitor cannot redact. So are queries with a custom type
/// modifier that isn't a plain number, such as `TEXT('x')` (see [`has_text_modifier`]).
pub fn redact_statement(statement: &Statement) -> ReString {
    match statement {
        Statement::Statement(statement) => {
            let mut redacted = statement.clone();
            if redacted.visit(&mut RedactLiterals).is_break() {
                return ReString::from_static("[SQL omitted]");
            }
            let redacted = redacted.to_restring();
            // Avoid token allocations for the common case without quoted text.
            if !redacted
                .as_str()
                .bytes()
                .any(|byte| matches!(byte, b'\'' | b'"' | b'$'))
            {
                return redacted;
            }
            // Some query AST fields (e.g. wildcard ILIKE patterns and ENUM labels)
            // store strings outside ValueWithSpan. Fail closed if formatting leaves
            // any string literal behind. Tokenization distinguishes quoted identifiers
            // from strings; the diagnostic SQL need not be parseable after redaction.
            match Tokenizer::new(&PostgreSqlDialect {}, redacted.as_str()).tokenize() {
                Ok(tokens) if !tokens.iter().any(is_string_literal) => redacted,
                _ => ReString::from_static("[SQL omitted]"),
            }
        }
        // EXPLAIN's extension options are not visited by sqlparser. Keep only the
        // query shape, rather than formatting potentially value-bearing options.
        Statement::Explain(explain) => {
            restate_util_string::format_restring!(
                "EXPLAIN {}",
                redact_statement(&explain.statement)
            )
        }
        _ => ReString::from_static("[SQL omitted]"),
    }
}

struct RedactLiterals;

fn is_string_literal(token: &Token) -> bool {
    matches!(
        token,
        Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::TripleSingleQuotedString(_)
            | Token::TripleDoubleQuotedString(_)
            | Token::DollarQuotedString(_)
            | Token::SingleQuotedByteStringLiteral(_)
            | Token::DoubleQuotedByteStringLiteral(_)
            | Token::TripleSingleQuotedByteStringLiteral(_)
            | Token::TripleDoubleQuotedByteStringLiteral(_)
            | Token::SingleQuotedRawStringLiteral(_)
            | Token::DoubleQuotedRawStringLiteral(_)
            | Token::TripleSingleQuotedRawStringLiteral(_)
            | Token::TripleDoubleQuotedRawStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::QuoteDelimitedStringLiteral(_)
            | Token::NationalQuoteDelimitedStringLiteral(_)
            | Token::EscapedStringLiteral(_)
            | Token::UnicodeStringLiteral(_)
            | Token::HexStringLiteral(_)
    )
}

/// The `LIMIT`, `OFFSET` and `FETCH` row counts of a query.
fn row_counts(query: &mut Query) -> impl Iterator<Item = &mut Expr> {
    let (limit, offset) = match &mut query.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => (
            limit.as_mut(),
            offset.as_mut().map(|offset| &mut offset.value),
        ),
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => (Some(limit), Some(offset)),
        None => (None, None),
    };
    let fetch = query
        .fetch
        .as_mut()
        .and_then(|fetch| fetch.quantity.as_mut());
    limit.into_iter().chain(offset).chain(fetch)
}

/// Whether a type, or a type nested in it, has a custom type modifier that isn't a plain
/// number. sqlparser stores modifiers such as the `'x'` in `TEXT('x')` as bare strings and
/// prints them without quotes, so neither the value redaction nor the final check for
/// string literals sees them. sqlparser has no visitor hook for types, so the visitor
/// checks every place a query can hold one.
fn has_text_modifier(data_type: &DataType) -> bool {
    match data_type {
        DataType::Custom(_, modifiers) => modifiers
            .iter()
            .any(|modifier| !modifier.bytes().all(|byte| byte.is_ascii_digit())),
        DataType::Array(
            ArrayElemTypeDef::AngleBracket(inner)
            | ArrayElemTypeDef::SquareBracket(inner, _)
            | ArrayElemTypeDef::Parenthesis(inner),
        )
        | DataType::Nullable(inner)
        | DataType::LowCardinality(inner) => has_text_modifier(inner),
        DataType::Map(key, value) => has_text_modifier(key) || has_text_modifier(value),
        DataType::Tuple(fields) | DataType::Struct(fields, _) => fields
            .iter()
            .any(|field| has_text_modifier(&field.field_type)),
        DataType::Union(fields) => fields
            .iter()
            .any(|field| has_text_modifier(&field.field_type)),
        DataType::Nested(columns)
        | DataType::NamedTable { columns, .. }
        | DataType::Table(Some(columns)) => columns
            .iter()
            .any(|column| has_text_modifier(&column.data_type)),
        _ => false,
    }
}

fn alias_has_text_modifier(alias: &TableAlias) -> bool {
    alias
        .columns
        .iter()
        .any(|column| column.data_type.as_ref().is_some_and(has_text_modifier))
}

fn json_columns_have_text_modifier(columns: &[JsonTableColumn]) -> bool {
    columns.iter().any(|column| match column {
        JsonTableColumn::Named(named) => has_text_modifier(&named.r#type),
        JsonTableColumn::ForOrdinality(_) => false,
        JsonTableColumn::Nested(nested) => json_columns_have_text_modifier(&nested.columns),
    })
}

fn arguments_have_text_modifier(arguments: &FunctionArguments) -> bool {
    matches!(arguments, FunctionArguments::List(list) if list.clauses.iter().any(|clause| {
        matches!(clause, FunctionArgumentClause::JsonReturningClause(returning)
            if has_text_modifier(&returning.data_type))
    }))
}

fn omit_if(leaks: bool) -> ControlFlow<()> {
    if leaks {
        ControlFlow::Break(())
    } else {
        ControlFlow::Continue(())
    }
}

impl VisitorMut for RedactLiterals {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut SqlStatement) -> ControlFlow<()> {
        match statement {
            SqlStatement::Query(_) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(()),
        }
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        // Runs before the query's values are visited. A plain-number row count becomes
        // an identifier with the same text, which `pre_visit_value` doesn't redact.
        // Expressions and placeholders stay values and are redacted as usual.
        for count in row_counts(query) {
            if let Expr::Value(ValueWithSpan {
                value: value @ Value::Number(..),
                ..
            }) = count
            {
                *count = Expr::Identifier(Ident::new(value.to_string()));
            }
        }
        // CTE column lists can carry types: `WITH c(a TEXT('x')) AS (...)`.
        omit_if(query.with.as_ref().is_some_and(|with| {
            with.cte_tables
                .iter()
                .any(|cte| alias_has_text_modifier(&cte.alias))
        }))
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        omit_if(match expr {
            Expr::Cast { data_type, .. } | Expr::TypedString(TypedString { data_type, .. }) => {
                has_text_modifier(data_type)
            }
            Expr::Convert { data_type, .. } => data_type.as_ref().is_some_and(has_text_modifier),
            Expr::Struct { fields, .. } => fields
                .iter()
                .any(|field| has_text_modifier(&field.field_type)),
            Expr::Lambda(lambda) => (&lambda.params)
                .into_iter()
                .any(|param| param.data_type.as_ref().is_some_and(has_text_modifier)),
            Expr::Function(function) => {
                arguments_have_text_modifier(&function.args)
                    || arguments_have_text_modifier(&function.parameters)
            }
            _ => false,
        })
    }

    fn pre_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<()> {
        // Exhaustive on purpose: a new kind of table factor must be checked for types too.
        omit_if(match table_factor {
            TableFactor::JsonTable { columns, alias, .. } => {
                json_columns_have_text_modifier(columns)
                    || alias.as_ref().is_some_and(alias_has_text_modifier)
            }
            TableFactor::OpenJsonTable { columns, alias, .. } => {
                columns
                    .iter()
                    .any(|column| has_text_modifier(&column.r#type))
                    || alias.as_ref().is_some_and(alias_has_text_modifier)
            }
            TableFactor::XmlTable { columns, alias, .. } => {
                columns.iter().any(|column| {
                    matches!(&column.option, XmlTableColumnOption::NamedInfo { r#type, .. }
                        if has_text_modifier(r#type))
                }) || alias.as_ref().is_some_and(alias_has_text_modifier)
            }
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::NestedJoin { alias, .. }
            | TableFactor::Pivot { alias, .. }
            | TableFactor::Unpivot { alias, .. }
            | TableFactor::MatchRecognize { alias, .. }
            | TableFactor::SemanticView { alias, .. } => {
                alias.as_ref().is_some_and(alias_has_text_modifier)
            }
        })
    }

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
        value.value = Value::Placeholder("?".into());
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion_sql::parser::DFParser;
    use datafusion_sql::sqlparser::dialect::{Dialect, MySqlDialect};

    use super::*;

    fn parse(sql: &str, dialect: &dyn Dialect) -> Statement {
        let mut statements = DFParser::parse_sql_with_dialect(sql, dialect).unwrap();
        assert_eq!(statements.len(), 1, "{sql}");
        statements.pop_front().unwrap()
    }

    #[test]
    fn redact_query_literals_without_changing_the_planning_ast() {
        for (sql, expected) in [
            (
                "SELECT * FROM sys_invocation WHERE target_service_key = 'customer-123' AND id IN ('inv_a', 'inv_b') LIMIT 20",
                "SELECT * FROM sys_invocation WHERE target_service_key = ? AND id IN (?, ?) LIMIT 20",
            ),
            (
                "SELECT DATE '2026-10-02', TIMESTAMP '2026-10-02 12:00:00', INTERVAL '7 days', TRUE, NULL, 12.34, -42",
                "SELECT DATE ?, TIMESTAMP ?, INTERVAL ?, ?, ?, ?, -?",
            ),
            (
                "SELECT $$sensitive$$, E'sensitive\\nvalue', 'Alice''s secret' /* secret comment */ -- another secret\n",
                "SELECT ?, ?, ?",
            ),
            (
                "WITH t AS (SELECT 'secret' AS value) SELECT * FROM t WHERE value LIKE '%private%' ESCAPE '!'",
                "WITH t AS (SELECT ? AS value) SELECT * FROM t WHERE value LIKE ? ESCAPE ?",
            ),
            (
                "SELECT CAST('private' AS VARCHAR(123)) AS \"visible-alias\" FROM \"visible-table\"",
                "SELECT CAST(? AS VARCHAR(123)) AS \"visible-alias\" FROM \"visible-table\"",
            ),
            ("SELECT $1, X'736563726574'", "SELECT ?, ?"),
            ("EXPLAIN ANALYZE SELECT 'secret'", "EXPLAIN SELECT ?"),
            ("COPY (SELECT 'secret') TO 'private-path'", "[SQL omitted]"),
            ("SET application_name = 'secret'", "[SQL omitted]"),
            ("SELECT CAST('secret' AS ENUM('private'))", "[SQL omitted]"),
            // Custom type modifiers are printed without quotes: omit them unless numeric.
            (
                "SELECT CAST(a AS TEXT('customer-123')) FROM t",
                "[SQL omitted]",
            ),
            ("SELECT * FROM t AS x(a my_type('secret'))", "[SQL omitted]"),
            (
                "WITH c(a TEXT('secret')) AS (SELECT 1) SELECT * FROM c",
                "[SQL omitted]",
            ),
            (
                "SELECT CAST(a AS my_type('secret')[]) FROM t",
                "[SQL omitted]",
            ),
            (
                "SELECT CAST(a AS my_type(10, 2)) FROM t AS x(a my_type(3))",
                "SELECT CAST(a AS my_type(10, 2)) FROM t AS x (a my_type(3))",
            ),
            // Plain-number row counts stay visible, in every query of the statement.
            (
                "SELECT * FROM t WHERE a = 7 ORDER BY a LIMIT 10 OFFSET 20",
                "SELECT * FROM t WHERE a = ? ORDER BY a LIMIT 10 OFFSET 20",
            ),
            (
                "SELECT * FROM t ORDER BY a OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY",
                "SELECT * FROM t ORDER BY a OFFSET 5 ROWS FETCH FIRST 10 ROWS ONLY",
            ),
            (
                "SELECT * FROM (SELECT a FROM t WHERE b = 'secret' LIMIT 3) AS s WHERE a IN (SELECT a FROM u OFFSET 4) LIMIT 1",
                "SELECT * FROM (SELECT a FROM t WHERE b = ? LIMIT 3) AS s WHERE a IN (SELECT a FROM u OFFSET 4) LIMIT 1",
            ),
            (
                "EXPLAIN SELECT 'secret' LIMIT 5",
                "EXPLAIN SELECT ? LIMIT 5",
            ),
            // Anything else in a row count is still redacted.
            (
                "SELECT * FROM t LIMIT length('secret') OFFSET $1",
                "SELECT * FROM t LIMIT length(?) OFFSET ?",
            ),
            (
                "SELECT * FROM t LIMIT 2 + 3 OFFSET -1",
                "SELECT * FROM t LIMIT ? + ? OFFSET -?",
            ),
            (
                "SELECT * FROM t FETCH FIRST $1 ROWS ONLY",
                "SELECT * FROM t FETCH FIRST ? ROWS ONLY",
            ),
        ] {
            let statement = parse(sql, &PostgreSqlDialect {});
            let original = statement.clone();
            assert_eq!(redact_statement(&statement).as_str(), expected, "{sql}");
            assert_eq!(statement, original);
        }

        // MySQL's `LIMIT offset, count` form, which the PostgreSQL dialect doesn't accept.
        let statement = parse(
            "SELECT * FROM t WHERE a = 'secret' LIMIT 5, 10",
            &MySqlDialect {},
        );
        assert_eq!(
            redact_statement(&statement).as_str(),
            "SELECT * FROM t WHERE a = ? LIMIT 5, 10"
        );
    }
}
