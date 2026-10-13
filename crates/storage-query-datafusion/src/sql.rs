// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::{
    Statement as SqlStatement, Value, ValueWithSpan, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

use restate_util_string::{ReString, ToReString};

/// Formats a diagnostic copy of the parsed query, never the AST used for planning.
/// Literals (including placeholders) become `?`; comments are discarded by the parser.
/// Identifiers, aliases, and type parameters remain visible. This is not executable SQL.
/// Statements outside the query subset are omitted: some store values as bare strings
/// that sqlparser's value visitor cannot redact.
pub(crate) fn redact_statement(statement: &Statement) -> ReString {
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

impl VisitorMut for RedactLiterals {
    type Break = ();

    fn pre_visit_statement(&mut self, statement: &mut SqlStatement) -> ControlFlow<()> {
        match statement {
            SqlStatement::Query(_) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(()),
        }
    }

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
        value.value = Value::Placeholder("?".into());
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::config::Dialect;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[test]
    fn redact_query_literals_without_changing_the_planning_ast() {
        let state = SessionContext::new().state();
        for (sql, expected) in [
            (
                "SELECT * FROM sys_invocation WHERE target_service_key = 'customer-123' AND id IN ('inv_a', 'inv_b') LIMIT 20",
                "SELECT * FROM sys_invocation WHERE target_service_key = ? AND id IN (?, ?) LIMIT ?",
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
        ] {
            let statement = state.sql_to_statement(sql, &Dialect::PostgreSQL).unwrap();
            let original = statement.clone();
            assert_eq!(redact_statement(&statement).as_str(), expected, "{sql}");
            assert_eq!(statement, original);
        }
    }
}
