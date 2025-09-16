// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use derive_builder::Builder;

use crate::BoxedCfMatcher;
use crate::configuration::{CfConfigurator, DbConfigurator};

type SmartString = smartstring::SmartString<smartstring::LazyCompact>;

#[derive(
    Debug,
    derive_more::Deref,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
)]
pub struct DbName(SmartString);
impl DbName {
    pub fn new(name: &str) -> Self {
        Self(name.into())
    }
}

impl From<restate_types::partitions::DbName> for DbName {
    fn from(name: restate_types::partitions::DbName) -> Self {
        let inner: SmartString = name.into();
        Self(inner)
    }
}

#[derive(
    Debug,
    derive_more::Deref,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
)]
pub struct CfName(SmartString);
impl CfName {
    pub fn new(name: &str) -> Self {
        Self(name.into())
    }
}

impl From<&str> for CfName {
    fn from(name: &str) -> Self {
        Self(name.into())
    }
}

impl AsRef<str> for CfName {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<restate_types::partitions::CfName> for CfName {
    fn from(name: restate_types::partitions::CfName) -> Self {
        let inner: SmartString = name.into();
        Self(inner)
    }
}

impl From<String> for CfName {
    fn from(name: String) -> Self {
        Self(name.into())
    }
}

pub trait CfNameMatch: std::fmt::Debug {
    fn cf_matches(&self, cf: &str) -> bool;
}

#[derive(Debug)]
pub struct CfPrefixPattern {
    prefix: SmartString,
}

impl CfPrefixPattern {
    pub const ANY: Self = Self {
        prefix: SmartString::new_const(),
    };

    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl CfNameMatch for CfPrefixPattern {
    fn cf_matches(&self, cf: &str) -> bool {
        self.prefix.is_empty() || cf.starts_with(&*self.prefix)
    }
}

#[derive(Debug)]
pub struct CfExactPattern {
    name: SmartString,
}

impl CfExactPattern {
    pub fn new(name: impl Into<SmartString>) -> Self {
        Self { name: name.into() }
    }
}

impl CfNameMatch for CfExactPattern {
    fn cf_matches(&self, cf: &str) -> bool {
        self.name == cf
    }
}

#[derive(Builder)]
#[builder(pattern = "owned", build_fn(name = "build"))]
pub struct DbSpec {
    pub(crate) name: DbName,
    pub(crate) path: PathBuf,
    /// All column families that should be flushed on shutdown, no flush will be performed if empty
    /// which should be the default for most cases.
    #[builder(default)]
    pub(crate) flush_on_shutdown: Vec<BoxedCfMatcher>,
    /// Ensure that those column families exist. It's the caller's responsibility to make sure that
    /// those column families have matchers defined in cf_patterns to configure them properly,
    /// otherwise opening the database will fail with `UnknownColumnFamily` error.
    #[builder(default)]
    pub(crate) ensure_column_families: Vec<CfName>,
    /// Configurator for the database-level options.
    pub(crate) db_configurator: Box<dyn DbConfigurator + Send + Sync>,
    /// Options of the column family are applied after the values loaded from
    /// RocksDbOptions from disk/env. Those act as column-family specific overrides for that
    /// particular pattern.
    ///
    /// Overriding per-column family options from config file is not supported.
    ///
    /// Patterns are checked in order to find the correct options to apply to the column family, if
    /// a column family didn't match any, opening the database or the column family will fail with
    /// `UnknownColumnFamily` error
    pub(crate) cf_patterns: Vec<(BoxedCfMatcher, Box<dyn CfConfigurator + Send + Sync>)>,
}

impl DbSpec {
    pub fn name(&self) -> &DbName {
        &self.name
    }
}

impl DbSpecBuilder {
    pub fn new(
        name: DbName,
        path: PathBuf,
        configurator: impl DbConfigurator + Send + Sync + 'static,
    ) -> DbSpecBuilder {
        Self {
            name: Some(name),
            path: Some(path),
            db_configurator: Some(Box::new(configurator)),
            ..Self::default()
        }
    }

    pub fn add_to_flush_on_shutdown(
        mut self,
        pattern: impl CfNameMatch + 'static + Send + Sync,
    ) -> Self {
        let mut cfs = self.flush_on_shutdown.unwrap_or_default();
        cfs.push(Box::new(pattern));
        self.flush_on_shutdown = Some(cfs);
        self
    }

    pub fn add_cf_pattern(
        mut self,
        pattern: impl CfNameMatch + Send + Sync + 'static,
        options: impl CfConfigurator + Send + Sync + 'static,
    ) -> Self {
        let mut cfs = self.cf_patterns.unwrap_or_default();
        cfs.push((Box::new(pattern), Box::new(options)));
        self.cf_patterns = Some(cfs);
        self
    }
}
