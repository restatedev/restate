// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use slotmap::{SecondaryMap, SlotMap};

use restate_util_string::{OwnedStringLike, StringLike};

use crate::rule::RuleHandle;
use crate::{Level, LimitKey, RulePattern};

#[derive(Debug, Clone, Default)]
pub enum Limit<V> {
    /// A limit is not defined at this level. The meaning of undefined depends on
    /// the use-case.
    #[default]
    Undefined,
    Defined(RuleHandle, V),
}

/// The concrete limits that apply for after matching a limit key to the set of
/// rules from the rule store.
#[derive(Debug)]
pub struct StructuredLimits<V> {
    scope: Limit<V>,
    l1: Limit<V>,
    l2: Limit<V>,
}

impl<V> Default for StructuredLimits<V> {
    fn default() -> Self {
        Self {
            scope: Limit::Undefined,
            l1: Limit::Undefined,
            l2: Limit::Undefined,
        }
    }
}

impl<S: OwnedStringLike, V> Rules<S, V> {
    /// Look up the limits for a given scope + limit key combination.
    #[inline]
    pub fn lookup(&self, scope: &str, limit_key: &LimitKey<S>) -> StructuredLimits<&V> {
        let mut limits = StructuredLimits::default();
        limits.scope = self
            .lookup_scope_rule(scope)
            .map(|handle| Limit::Defined(handle, self.limits.get(handle).unwrap()))
            .unwrap_or(Limit::Undefined);

        match limit_key {
            LimitKey::None => {}
            limit @ LimitKey::L1(_) => {
                limits.l1 = self
                    .lookup_l1_rule(scope, limit)
                    .map(|handle| Limit::Defined(handle, self.limits.get(handle).unwrap()))
                    .unwrap_or(Limit::Undefined);
            }
            limit @ LimitKey::L2(_, _) => {
                limits.l2 = self
                    .lookup_l2_rule(scope, limit)
                    .map(|handle| Limit::Defined(handle, self.limits.get(handle).unwrap()))
                    .unwrap_or(Limit::Undefined);
            }
        }

        limits
    }

    pub fn get_limit(&self, handle: RuleHandle) -> Option<&V> {
        self.limits.get(handle)
    }

    pub fn get_pattern(&self, handle: RuleHandle) -> Option<&RulePattern<S>> {
        self.rules.get(handle)
    }

    pub fn get(&self, handle: RuleHandle) -> Option<(&RulePattern<S>, &V)> {
        let rule = self.rules.get(handle)?;
        self.limits.get(handle).map(|l| (rule, l))
    }

    fn lookup_scope_rule(&self, scope: &str) -> Option<RuleHandle> {
        self.scope_rules
            .iter()
            .filter_map(|handle| {
                let rank = self.rules.get(*handle)?.rank(scope, &LimitKey::None)?;
                Some((*handle, rank))
            })
            .max_by_key(|&(_, y)| y)
            .map(|(x, _)| x)
    }

    fn lookup_l1_rule(&self, scope: &str, limit_key: &LimitKey<S>) -> Option<RuleHandle> {
        self.l1_rules
            .iter()
            .filter_map(|handle| {
                let rank = self.rules.get(*handle)?.rank(scope, limit_key)?;
                Some((*handle, rank))
            })
            .max_by_key(|&(_, y)| y)
            .map(|(x, _)| x)
    }

    fn lookup_l2_rule(&self, scope: &str, limit_key: &LimitKey<S>) -> Option<RuleHandle> {
        self.l2_rules
            .iter()
            .filter_map(|handle| {
                let rank = self.rules.get(*handle)?.rank(scope, limit_key)?;
                Some((*handle, rank))
            })
            .max_by_key(|&(_, y)| y)
            .map(|(x, _)| x)
    }
}

#[derive(Debug)]
pub struct Rules<S: StringLike, V> {
    rules: SlotMap<RuleHandle, RulePattern<S>>,
    limits: SecondaryMap<RuleHandle, V>,

    scope_rules: Vec<RuleHandle>,
    l1_rules: Vec<RuleHandle>,
    l2_rules: Vec<RuleHandle>,
}

impl<S: StringLike, V> Rules<S, V> {
    /// Create a rule store from an iterator of rules.
    pub fn from_rules(rules: impl IntoIterator<Item = (RulePattern<S>, V)>) -> Self {
        let mut store = Self::default();
        for (pattern, limit) in rules {
            store.add_rule(pattern, limit);
        }
        store
    }
    /// Add a rule to the store.
    pub fn add_rule(&mut self, pattern: RulePattern<S>, limit: V) {
        let level = pattern.level();
        let handle = self.rules.insert(pattern);
        self.limits.insert(handle, limit);

        match level {
            Level::Scope => self.scope_rules.push(handle),
            Level::Level1 => self.l1_rules.push(handle),
            Level::Level2 => self.l2_rules.push(handle),
        }
    }

    /// Clear all rules.
    pub fn clear(&mut self) {
        self.scope_rules.clear();
        self.l1_rules.clear();
        self.l2_rules.clear();
        self.limits.clear();
        self.rules.clear();
    }
}

impl<S: StringLike, V> Default for Rules<S, V> {
    fn default() -> Self {
        Self {
            scope_rules: Default::default(),
            l1_rules: Default::default(),
            l2_rules: Default::default(),
            limits: Default::default(),
            rules: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_with_rules(specs: &[(&str, u32)]) -> Rules<String, u32> {
        Rules::from_rules(
            specs
                .iter()
                .map(|(pat, limit)| (pat.parse::<RulePattern<String>>().unwrap(), *limit)),
        )
    }

    #[test]
    fn add_and_retrieve_rules() {
        let store = store_with_rules(&[
            ("*", 1000),
            ("scope1", 500),
            ("scope1/*", 100),
            ("scope1/tenant1", 50),
            ("scope1/*/*", 10),
            ("scope1/tenant1/user1", 5),
        ]);

        // Verify rules are categorized by level
        assert_eq!(store.scope_rules.len(), 2);
        assert_eq!(store.l1_rules.len(), 2);
        assert_eq!(store.l2_rules.len(), 2);

        // Verify get/get_limit/get_pattern round-trip through handles
        for &handle in &store.scope_rules {
            let (pat, limit) = store.get(handle).unwrap();
            assert_eq!(pat.level(), Level::Scope);
            assert_eq!(store.get_limit(handle), Some(limit));
            assert_eq!(store.get_pattern(handle), Some(pat));
        }
    }

    #[test]
    fn clear_removes_everything() {
        let mut store = store_with_rules(&[("*", 1000), ("scope1/*", 100)]);
        assert_eq!(store.scope_rules.len(), 1);
        assert_eq!(store.l1_rules.len(), 1);

        store.clear();
        assert!(store.scope_rules.is_empty());
        assert!(store.l1_rules.is_empty());
        assert!(store.l2_rules.is_empty());
        assert!(store.rules.is_empty());
        assert!(store.limits.is_empty());
    }

    #[test]
    fn lookup_scope_prefers_exact_over_wildcard() {
        let store = store_with_rules(&[("*", 1000), ("scope1", 500)]);

        // Exact match wins for scope1
        let result = store.lookup("scope1", &LimitKey::None);
        assert!(matches!(result.scope, Limit::Defined(_, 500)));

        // Wildcard used for unknown scope
        let result = store.lookup("other", &LimitKey::None);
        assert!(matches!(result.scope, Limit::Defined(_, 1000)));
    }

    #[test]
    fn lookup_l1_precedence() {
        let store = store_with_rules(&[
            ("*/*", 1000),
            ("scope1/*", 100),
            ("*/tenant1", 200),
            ("scope1/tenant1", 50),
        ]);

        let key: LimitKey<String> = "tenant1".parse().unwrap();

        // Exact scope + exact L1 wins
        let result = store.lookup("scope1", &key);
        assert!(matches!(result.l1, Limit::Defined(_, 50)));
        assert!(matches!(result.l2, Limit::Undefined));

        // Wildcard scope + exact L1
        let result = store.lookup("other", &key);
        assert!(matches!(result.l1, Limit::Defined(_, 200)));

        // Exact scope + wildcard L1
        let other_key: LimitKey<String> = "other_tenant".parse().unwrap();
        let result = store.lookup("scope1", &other_key);
        assert!(matches!(result.l1, Limit::Defined(_, 100)));

        // Wildcard scope + wildcard L1
        let result = store.lookup("other", &other_key);
        assert!(matches!(result.l1, Limit::Defined(_, 1000)));
    }

    #[test]
    fn lookup_l2_precedence() {
        let store = store_with_rules(&[
            ("*/*/*", 1000),
            ("scope1/*/*", 500),
            ("scope1/org1/*", 100),
            ("scope1/org1/user1", 10),
        ]);

        let key: LimitKey<String> = "org1/user1".parse().unwrap();

        // Full exact match
        let result = store.lookup("scope1", &key);
        assert!(matches!(result.l2, Limit::Defined(_, 10)));

        // scope1/org1/* for unknown user
        let key2: LimitKey<String> = "org1/user99".parse().unwrap();
        let result = store.lookup("scope1", &key2);
        assert!(matches!(result.l2, Limit::Defined(_, 100)));

        // scope1/*/* for unknown org
        let key3: LimitKey<String> = "org99/user1".parse().unwrap();
        let result = store.lookup("scope1", &key3);
        assert!(matches!(result.l2, Limit::Defined(_, 500)));

        // */*/* for unknown scope
        let result = store.lookup("other", &key);
        assert!(matches!(result.l2, Limit::Defined(_, 1000)));
    }

    #[test]
    fn lookup_returns_undefined_when_no_rules_match() {
        let store: Rules<String, u32> = Rules::default();

        let result = store.lookup("scope1", &LimitKey::None);
        assert!(matches!(result.scope, Limit::Undefined));

        let key: LimitKey<String> = "tenant1".parse().unwrap();
        let result = store.lookup("scope1", &key);
        assert!(matches!(result.l1, Limit::Undefined));
    }

    #[test]
    fn lookup_only_populates_matching_depth() {
        let store = store_with_rules(&[("*", 1000), ("*/*", 100), ("*/*/*", 10)]);

        // Scope-only query: only scope is populated
        let result = store.lookup("s", &LimitKey::None);
        assert!(matches!(result.scope, Limit::Defined(_, 1000)));
        assert!(matches!(result.l1, Limit::Undefined));
        assert!(matches!(result.l2, Limit::Undefined));

        // L1 query: scope + l1
        let key: LimitKey<String> = "t".parse().unwrap();
        let result = store.lookup("s", &key);
        assert!(matches!(result.scope, Limit::Defined(_, 1000)));
        assert!(matches!(result.l1, Limit::Defined(_, 100)));
        assert!(matches!(result.l2, Limit::Undefined));

        // L2 query: scope + l2 (l1 stays undefined since lookup branches on key variant)
        let key: LimitKey<String> = "t/u".parse().unwrap();
        let result = store.lookup("s", &key);
        assert!(matches!(result.scope, Limit::Defined(_, 1000)));
        assert!(matches!(result.l1, Limit::Undefined));
        assert!(matches!(result.l2, Limit::Defined(_, 10)));
    }
}
