// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rule types and storage for the hierarchical limiter.

use std::str::FromStr;

use restate_util_string::{OwnedStringLike, RestrictedValue, RestrictedValueError, StringLike};

use crate::{Level, LimitKey};

slotmap::new_key_type! { pub struct RuleHandle; }

/// Error type for pattern parsing failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// The pattern is empty.
    #[error("pattern cannot be empty")]
    EmptyPattern,
    /// A component is invalid.
    #[error("invalid component: {0}")]
    InvalidComponent(#[from] RestrictedValueError),
    /// Too many path components (max 3).
    #[error("too many path components (max 3)")]
    TooManyComponents,
}

/// To Parse a pattern
/// This represents the "key" part of a rule, without the limit value.
///
/// let pattern: RulePattern<String> = "scope1/*".parse().unwrap();
///
/// # Format for parsing rule patterns
///
/// Patterns specify which keys a rule applies to. SCOPE can be an exact value or
/// a wildcard (`*`) for global defaults:
///
/// - `scope`       - Scope exact: matches specific scope
/// - `*`           - Scope wildcard: default for any scope (global defaults)
/// - `scope/*`     - Level 1 wildcard: matches any L1 under `scope`
/// - `scope/l1`    - Level 1 exact: matches `l1` under `scope`
/// - `*/l1`        - Level 1 exact with wildcard scope: default for specific L1 under any scope
/// - `scope/*/*`   - Level 2 wildcard: matches any L2 under `scope`
/// - `scope/*/l2`  - Level 2 with wildcard L1: matches specific L2 across all L1
/// - `scope/l1/*`  - Level 2 with wildcard L2: matches any L2 under specific L1 and `scope`
/// - `scope/l1/l2` - Level 2 exact: matches specific L1 and L1 and `scope`
///
/// The wildcard character is `*`. Component values must be valid restricted values
/// (only `[a-zA-Z0-9_.-]`, non-empty, max 36 bytes).
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum RulePattern<S: StringLike> {
    /// A rule that defines a limit for the scope level
    Scope(Pattern<S>),
    /// A rule that defines a limit for level 1 of the limit key
    L1 { scope: Pattern<S>, l1: Pattern<S> },
    /// A rule that defines a limit for level 2 of the limit key
    L2 {
        scope: Pattern<S>,
        l1: Pattern<S>,
        l2: Pattern<S>,
    },
}

impl<S: StringLike> RulePattern<S> {
    /// Returns the level of this pattern
    pub fn level(&self) -> Level {
        match self {
            RulePattern::Scope(_) => Level::Scope,
            RulePattern::L1 { .. } => Level::Level1,
            RulePattern::L2 { .. } => Level::Level2,
        }
    }

    /// Computes a precedence rank for this pattern against the given scope and
    /// limit key. Returns `None` if the pattern does not match the input.
    ///
    /// The rank is a bitmask where each bit corresponds to a hierarchy level
    /// (scope = bit 2, L1 = bit 1, L2 = bit 0). An exact match at a level sets
    /// its bit to 1; a wildcard leaves it at 0. Higher rank values indicate more
    /// specific matches and should take precedence.
    ///
    /// A pattern only matches inputs at the same depth — an L1 pattern requires
    /// the limit key to have at least one component, an L2 pattern requires two.
    ///
    /// # Precedence table (for L2 patterns)
    ///
    /// | Rank | Scope | L1  | L2  | Example             |
    /// |------|-------|-----|-----|---------------------|
    /// |  0   |  `*`  | `*` | `*` | `*/*/*`             |
    /// |  1   |  `*`  | `*` | exact | `*/*/bar`         |
    /// |  2   |  `*`  | exact | `*` | `*/foo/*`         |
    /// |  3   |  `*`  | exact | exact | `*/foo/bar`     |
    /// |  4   | exact | `*` | `*` | `scope/*/*`         |
    /// |  5   | exact | `*` | exact | `scope/*/bar`     |
    /// |  6   | exact | exact | `*` | `scope/foo/*`     |
    /// |  7   | exact | exact | exact | `scope/foo/bar` |
    pub fn rank(&self, scope: &str, input_limit_key: &LimitKey<S>) -> Option<u8> {
        // Precedence Rules (lowest to highest):
        //  0 0 0  *     / *   / *    = 0
        //  0 0 1  *     / *   / bar  = 1
        //  0 1 0  *     / foo / *    = 2
        //  0 1 1  *     / foo / bar  = 3
        //  1 0 0  scope / *   / *    = 4
        //  1 0 1  scope / *   / bar  = 5
        //  1 1 0  scope / foo / *    = 6
        //  1 1 1  scope / foo / bar  = 7
        match self {
            // depth = 0
            RulePattern::Scope(pat) => pat.rank(scope, Level::Scope),
            // depth = 2
            RulePattern::L1 {
                scope: scope_pat,
                l1,
            } => {
                let mut result = scope_pat.rank(scope, Level::Scope)?;
                result |= l1.rank(input_limit_key.level1()?.as_str(), Level::Level1)?;
                Some(result)
            }
            // depth = 3
            RulePattern::L2 {
                scope: scope_pat,
                l1,
                l2,
            } => {
                let mut result = scope_pat.rank(scope, Level::Scope)?;
                result |= l1.rank(input_limit_key.level1()?.as_str(), Level::Level1)?;
                result |= l2.rank(input_limit_key.level2()?.as_str(), Level::Level2)?;
                Some(result)
            }
        }
    }
}

/// Builds a rule with a wildcard scope (matches any scope value).
impl<S: StringLike> Default for RulePattern<S> {
    fn default() -> Self {
        Self::Scope(Pattern::Wildcard)
    }
}

impl<S: OwnedStringLike> FromStr for RulePattern<S> {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_rule_pattern(s)
    }
}

impl<S: StringLike> std::fmt::Display for RulePattern<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scope(p) => std::fmt::Display::fmt(p, f),
            Self::L1 { scope, l1 } => write!(f, "{scope}/{l1}"),
            Self::L2 { scope, l1, l2 } => write!(f, "{scope}/{l1}/{l2}"),
        }
    }
}

/// A pattern component that matches either an exact value or any value (wildcard).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Pattern<S: StringLike> {
    /// Match a specific value.
    Exact(RestrictedValue<S>),
    /// Match any value (wildcard).
    #[default]
    Wildcard,
}

impl<S: StringLike> Pattern<S> {
    pub fn len(&self) -> usize {
        match self {
            Self::Exact(k) => k.as_str().len(),
            Self::Wildcard => 1,
        }
    }

    /// Matches the input and returns a ranking value's most signifcant bit
    fn rank(&self, input: &str, level: Level) -> Option<u8> {
        let msb = Level::COUNT - level as usize;
        match self {
            Self::Exact(pat) => (*pat == *input).then_some(1 << msb),
            Self::Wildcard => Some(0),
        }
    }
}

impl<S: StringLike> AsRef<str> for Pattern<S> {
    fn as_ref(&self) -> &str {
        match self {
            Self::Exact(k) => k.as_ref(),
            Self::Wildcard => "*",
        }
    }
}

impl<S: StringLike> std::fmt::Display for Pattern<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact(v) => std::fmt::Display::fmt(v, f),
            Self::Wildcard => write!(f, "*"),
        }
    }
}

/// Parse a rule pattern from a string.
pub fn parse_rule_pattern<S: OwnedStringLike>(s: &str) -> Result<RulePattern<S>, ParseError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ParseError::EmptyPattern);
    }

    // Split the pattern into components
    let mut next_idx = 0;
    let mut parts: [Option<Pattern<S>>; 3] = [None, None, None];

    for part in s.split_terminator('/') {
        if next_idx >= 3 {
            return Err(ParseError::TooManyComponents);
        }
        let pattern = parse_pattern_component(part)?;
        parts[next_idx] = Some(pattern);
        next_idx += 1;
    }

    match next_idx {
        0 => Err(ParseError::EmptyPattern),
        1 => Ok(RulePattern::Scope(parts[0].take().unwrap())),
        2 => Ok(RulePattern::L1 {
            scope: parts[0].take().unwrap(),
            l1: parts[1].take().unwrap(),
        }),
        3 => Ok(RulePattern::L2 {
            scope: parts[0].take().unwrap(),
            l1: parts[1].take().unwrap(),
            l2: parts[2].take().unwrap(),
        }),
        _ => Err(ParseError::TooManyComponents),
    }
}

fn parse_pattern_component<S: OwnedStringLike>(s: &str) -> Result<Pattern<S>, ParseError> {
    let s = s.trim();
    if s == "*" {
        return Ok(Pattern::Wildcard);
    }
    Ok(Pattern::Exact(RestrictedValue::from_str(s)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_scope_patterns() {
        let pattern: RulePattern<String> = "scope1".parse().unwrap();
        assert_eq!(pattern.level(), Level::Scope);
        assert_eq!(pattern.to_string(), "scope1");
        assert_eq!(
            pattern,
            RulePattern::Scope(Pattern::Exact("scope1".parse().unwrap())),
        );

        // Bad scope
        let err = "s*".parse::<RulePattern<String>>().unwrap_err();
        assert!(matches!(
            err,
            ParseError::InvalidComponent(RestrictedValueError::InvalidChar('*')),
        ));

        let err = "".parse::<RulePattern<String>>().unwrap_err();
        assert!(matches!(err, ParseError::EmptyPattern));

        let err = "/".parse::<RulePattern<String>>().unwrap_err();
        assert!(matches!(
            err,
            ParseError::InvalidComponent(RestrictedValueError::Empty),
        ));

        let err = "/*".parse::<RulePattern<String>>().unwrap_err();
        assert!(matches!(
            err,
            ParseError::InvalidComponent(RestrictedValueError::Empty),
        ));
    }

    #[test]
    fn parse_level1() {
        let pattern: RulePattern<String> = "scope1/*".parse().unwrap();
        assert_eq!(pattern.level(), Level::Level1);
        assert_eq!(pattern.to_string(), "scope1/*");
        assert_eq!(
            pattern,
            RulePattern::L1 {
                scope: Pattern::Exact("scope1".parse().unwrap()),
                l1: Pattern::Wildcard,
            }
        );

        let pattern: RulePattern<String> = "scope1/tenant1".parse().unwrap();
        assert_eq!(pattern.level(), Level::Level1);
        assert_eq!(pattern.to_string(), "scope1/tenant1");
        assert_eq!(
            pattern,
            RulePattern::L1 {
                scope: Pattern::Exact("scope1".parse().unwrap()),
                l1: Pattern::Exact("tenant1".parse().unwrap()),
            }
        );

        // a trailing / doesn't matter
        let pattern: RulePattern<String> = "scope1/*/".parse().unwrap();
        assert_eq!(pattern.level(), Level::Level1);
        assert_eq!(pattern.to_string(), "scope1/*");
        assert_eq!(
            pattern,
            RulePattern::L1 {
                scope: Pattern::Exact("scope1".parse().unwrap()),
                l1: Pattern::Wildcard,
            }
        );
    }

    #[test]
    fn parse_level2() {
        let pattern: RulePattern<String> = "scope1/*/*".parse().unwrap();
        assert_eq!(pattern.level(), Level::Level2);
        assert_eq!(pattern.to_string(), "scope1/*/*");
        assert_eq!(
            pattern,
            RulePattern::L2 {
                scope: Pattern::Exact("scope1".parse().unwrap()),
                l1: Pattern::Wildcard,
                l2: Pattern::Wildcard,
            }
        );

        let pattern: RulePattern<String> = "scope1/tenant1/*".parse().unwrap();
        assert_eq!(pattern.level(), Level::Level2);
        assert_eq!(pattern.to_string(), "scope1/tenant1/*");
        assert_eq!(
            pattern,
            RulePattern::L2 {
                scope: Pattern::Exact("scope1".parse().unwrap()),
                l1: Pattern::Exact("tenant1".parse().unwrap()),
                l2: Pattern::Wildcard,
            }
        );

        let pattern: RulePattern<String> = "scope1/*/user1".parse().unwrap();
        assert_eq!(pattern.level(), Level::Level2);
        assert_eq!(pattern.to_string(), "scope1/*/user1");
        assert_eq!(
            pattern,
            RulePattern::L2 {
                scope: Pattern::Exact("scope1".parse().unwrap()),
                l1: Pattern::Wildcard,
                l2: Pattern::Exact("user1".parse().unwrap()),
            }
        );

        let pattern: RulePattern<String> = "*/*/*".parse().unwrap();
        assert_eq!(pattern.level(), Level::Level2);
        assert_eq!(pattern.to_string(), "*/*/*");
        assert_eq!(
            pattern,
            RulePattern::L2 {
                scope: Pattern::Wildcard,
                l1: Pattern::Wildcard,
                l2: Pattern::Wildcard,
            }
        );

        // a trailing / doesn't matter
        let pattern: RulePattern<String> = "scope1/*/*/".parse().unwrap();
        assert_eq!(pattern.level(), Level::Level2);
        assert_eq!(pattern.to_string(), "scope1/*/*");
        assert_eq!(
            pattern,
            RulePattern::L2 {
                scope: Pattern::Exact("scope1".parse().unwrap()),
                l1: Pattern::Wildcard,
                l2: Pattern::Wildcard,
            }
        );
    }

    #[test]
    fn ranking_no_match() {
        // raw check
        let pattern: Pattern<String> = Pattern::Exact("abc".parse().unwrap());
        // not a match
        assert_eq!(pattern.rank("xyz", Level::Scope), None);

        // pattern no match
        let pattern: RulePattern<String> = "scope1/*".parse().unwrap();
        assert!(matches!(pattern.level(), Level::Level1));

        assert_eq!(pattern.rank("jim", &LimitKey::<_>::None), None);

        // This is a Level1 rule, input must be 2 levels-deep (scope + L1)
        assert_eq!(pattern.rank("scope1", &LimitKey::<_>::None), None);

        let pattern: RulePattern<String> = "*/*".parse().unwrap();
        // Same thing, This is a Level1 rule, input must be 2 levels-deep (scope + L1)
        assert_eq!(pattern.rank("scope1", &LimitKey::<_>::None), None);

        let pattern: RulePattern<String> = "*/*/*".parse().unwrap();
        // This is a Level2 rule, input must be 3 levels-deep (scope + L1 + L2)
        assert_eq!(pattern.rank("scope1", &LimitKey::<_>::None), None);
        assert_eq!(
            pattern.rank("scope1", &LimitKey::<_>::L1("foo".parse().unwrap())),
            None
        );
    }

    #[test]
    fn ranking_raw() {
        let pattern: Pattern<String> = Pattern::Wildcard;
        // wildcard's MSB is zero, always.
        assert_eq!(pattern.rank("abc", Level::Scope), Some(0));
        assert_eq!(pattern.rank("abc", Level::Level1), Some(0));
        assert_eq!(pattern.rank("abc", Level::Level2), Some(0));

        let pattern: Pattern<String> = Pattern::Exact("abc".parse().unwrap());
        assert_eq!(pattern.rank("abc", Level::Scope), Some(4));
        assert_eq!(pattern.rank("abc", Level::Level1), Some(2));
        assert_eq!(pattern.rank("abc", Level::Level2), Some(1));
    }

    #[test]
    fn scope_ranking() {
        // scope-level rules
        let rules: &[RulePattern<String>] = &["*".parse().unwrap(), "scope1".parse().unwrap()];
        let ranks = rules
            .iter()
            .map(|r| r.rank("scope1", &LimitKey::<_>::None))
            .collect::<Vec<_>>();

        assert_eq!(ranks, [Some(0), Some(4)]);
    }

    #[test]
    fn l1_ranking() {
        // l1 rules
        let rules: &[RulePattern<String>] = &[
            "*/*".parse().unwrap(),
            "scope1/*".parse().unwrap(),
            "*/tenant1".parse().unwrap(),
            "scope1/tenant1".parse().unwrap(),
            "unmatched/tenant1".parse().unwrap(),
        ];

        // scope1/tenant1 scores the highest (exact match)
        // but scope1/ wins over */tenant1 and over */*
        let ranks = rules
            .iter()
            .map(|r| r.rank("scope1", &"tenant1".parse().unwrap()))
            .collect::<Vec<_>>();

        assert_eq!(ranks, [Some(0), Some(4), Some(2), Some(6), None]);
    }

    #[test]
    fn l2_ranking() {
        // l2 rules
        let rules: &[RulePattern<String>] = &[
            "*/*/*".parse().unwrap(),
            "scope1/*/*".parse().unwrap(),
            "*/org1/*".parse().unwrap(),
            "*/*/tenant1".parse().unwrap(),
            "scope1/org1/*".parse().unwrap(),
            "scope1/org1/tenant1".parse().unwrap(),
            "unmatched/org1/tenant1".parse().unwrap(),
        ];

        // scope1/org1/tenant1 scores the highest (exact match)
        // Effective Precedence (for input scope1/org1/tenant1)
        //   - scope1/org1/tenant1 = 7
        //   - scope1/org1/* = 6
        //   - scope1/*/*  (scope has higher precedence than L1) = 4
        //   - */org1/*  = 2
        //   - */*/tenant1 = 1
        //   - */*/*  = 0
        //
        let ranks = rules
            .iter()
            .map(|r| r.rank("scope1", &"org1/tenant1".parse().unwrap()))
            .collect::<Vec<_>>();

        assert_eq!(
            ranks,
            [Some(0), Some(4), Some(2), Some(1), Some(6), Some(7), None]
        );
    }
}
