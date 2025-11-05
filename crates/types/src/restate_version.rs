// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::{Borrow, Cow};
use std::str::FromStr;
use std::sync::LazyLock;

use serde_with::serde_as;

use restate_encoding::BilrostNewType;

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct RestateVersionError(#[from] semver::Error);

/// Version of a restate binary
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, BilrostNewType)]
#[serde(transparent)]
pub struct RestateVersion(Cow<'static, str>);

impl RestateVersion {
    pub const UNKNOWN_STR: &str = "0.0.0-unknown";

    /// The current version of the currently running binary
    pub fn current() -> Self {
        Self(Cow::Borrowed(env!("CARGO_PKG_VERSION")))
    }

    pub const fn unknown() -> Self {
        // We still provide a semver valid version here
        Self(Cow::Borrowed(Self::UNKNOWN_STR))
    }

    pub fn new(s: String) -> Self {
        Self(Cow::Owned(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0.into_owned()
    }
}

impl AsRef<str> for RestateVersion {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl Borrow<str> for RestateVersion {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

/// A parsed semantic version of a restate binary
#[serde_as]
#[derive(Clone, serde::Serialize, serde::Deserialize, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct SemanticRestateVersion(#[serde_as(as = "serde_with::DisplayFromStr")] semver::Version);

restate_encoding::bilrost_as_display_from_str!(SemanticRestateVersion);

impl Default for SemanticRestateVersion {
    fn default() -> Self {
        Self::unknown()
    }
}

impl SemanticRestateVersion {
    pub fn current() -> &'static Self {
        static CURRENT_SEMVER: LazyLock<SemanticRestateVersion> = LazyLock::new(|| {
            SemanticRestateVersion(semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap())
        });
        &CURRENT_SEMVER
    }

    pub fn unknown() -> Self {
        Self(semver::Version::parse("0.0.0-unknown").unwrap())
    }

    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self(semver::Version::new(major, minor, patch))
    }

    pub fn major(&self) -> u64 {
        self.0.major
    }

    pub fn minor(&self) -> u64 {
        self.0.minor
    }

    pub fn patch(&self) -> u64 {
        self.0.patch
    }

    pub fn is_dev(&self) -> bool {
        self.0.pre == semver::Prerelease::new("dev").unwrap()
    }

    pub fn parse(s: &str) -> Result<Self, RestateVersionError> {
        Self::from_str(s)
    }

    /// Compare the major, minor, patch, and pre-release value of two versions,
    /// disregarding build metadata. Versions that differ only in build metadata
    /// are considered equal. This comparison is what the SemVer spec refers to
    /// as "precedence".
    ///
    /// # Example
    ///
    /// ```
    /// use restate_types::SemanticRestateVersion;
    ///
    /// let mut versions = [
    ///     "1.20.0+c144a98".parse::<SemanticRestateVersion>().unwrap(),
    ///     "1.20.0".parse().unwrap(),
    ///     "1.0.0".parse().unwrap(),
    ///     "1.0.0-alpha".parse().unwrap(),
    ///     "1.20.0+bc17664".parse().unwrap(),
    /// ];
    ///
    /// // This is a stable sort, so it preserves the relative order of equal
    /// // elements. The three 1.20.0 versions differ only in build metadata so
    /// // they are not reordered relative to one another.
    /// versions.sort_by(SemanticRestateVersion::cmp_precedence);
    /// assert_eq!(versions, [
    ///     "1.0.0-alpha".parse().unwrap(),
    ///     "1.0.0".parse().unwrap(),
    ///     "1.20.0+c144a98".parse().unwrap(),
    ///     "1.20.0".parse().unwrap(),
    ///     "1.20.0+bc17664".parse().unwrap(),
    /// ]);
    ///
    /// // Totally order the versions, including comparing the build metadata.
    /// versions.sort();
    /// assert_eq!(versions, [
    ///     "1.0.0-alpha".parse().unwrap(),
    ///     "1.0.0".parse().unwrap(),
    ///     "1.20.0".parse().unwrap(),
    ///     "1.20.0+bc17664".parse().unwrap(),
    ///     "1.20.0+c144a98".parse().unwrap(),
    /// ]);
    /// ```
    pub fn cmp_precedence(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp_precedence(&other.0)
    }

    /// True if this version is equal or newer than the other version
    ///
    /// note that a released version is considered newer than the equivalent dev version
    pub fn is_equal_or_newer_than(&self, other: &Self) -> bool {
        self.cmp_precedence(other) != std::cmp::Ordering::Less
    }

    /// True if this version is newer than the other version
    ///
    /// note that a released version is considered newer than the equivalent dev version
    pub fn is_newer_than(&self, other: &Self) -> bool {
        self.cmp_precedence(other) == std::cmp::Ordering::Greater
    }
}

impl std::fmt::Display for SemanticRestateVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Debug for SemanticRestateVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for SemanticRestateVersion {
    type Err = RestateVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let version = semver::Version::parse(s).map_err(RestateVersionError::from)?;
        Ok(Self(version))
    }
}

impl From<semver::Version> for SemanticRestateVersion {
    fn from(value: semver::Version) -> Self {
        Self(value)
    }
}

impl TryFrom<&RestateVersion> for SemanticRestateVersion {
    type Error = RestateVersionError;

    fn try_from(value: &RestateVersion) -> Result<Self, Self::Error> {
        let version = semver::Version::parse(value.borrow()).map_err(RestateVersionError::from)?;
        Ok(Self(version))
    }
}

// Version constants

/// Why isn't this value simply v1.6.0? The problem is that we use this value for feature gating,
/// and we want to be able to test features when we are still on 1.6.0-dev. To make this work, we need
/// a version X with 1.6.0 >= X && 1.6.0-dev >= X && 1.5.x < X. That's why we chose 1.6.0-dev.
///
/// An alternative approach could have been to use v1.5.u64::MAX instead as this version would
/// fulfill the same inequalities. However, we serialize SemanticRestateVersion by using
/// [`serde_with::DisplayFromStr`] and this would create quite a long string when one of the
/// components is `u64::MAX` (translates into 20 characters).
pub static RESTATE_VERSION_1_6_0: LazyLock<SemanticRestateVersion> =
    LazyLock::new(|| SemanticRestateVersion::parse("1.6.0-dev").expect("valid semver version"));

/// Why isn't this value simply v1.7.0? See description of [`RESTATE_VERSION_1_6_0`].
pub static RESTATE_VERSION_1_7_0: LazyLock<SemanticRestateVersion> =
    LazyLock::new(|| SemanticRestateVersion::parse("1.7.0-dev").expect("valid semver version"));

#[cfg(test)]
mod tests {
    use super::*;
    use googletest::prelude::*;

    #[test]
    fn restate_semantic_version() {
        let cargo_version = env!("CARGO_PKG_VERSION");
        let plain_version = RestateVersion::current();
        assert_that!(cargo_version, eq(plain_version.as_str()));
        let parsed = SemanticRestateVersion::from_str(cargo_version).unwrap();
        assert_that!(parsed.to_string(), eq(plain_version.as_str()));
        let parsed = SemanticRestateVersion::try_from(&RestateVersion::current()).unwrap();
        assert_that!(parsed.to_string(), eq(plain_version.as_str()));

        let parsed = SemanticRestateVersion::from_str("1.66.0").unwrap();
        assert_that!(parsed.major(), eq(1));
        assert_that!(parsed.minor(), eq(66));
        assert_that!(parsed.patch(), eq(0));
        assert_that!(parsed.is_dev(), eq(false));

        let parsed = SemanticRestateVersion::from_str("1.66.0-dev").unwrap();
        assert_that!(parsed.major(), eq(1));
        assert_that!(parsed.minor(), eq(66));
        assert_that!(parsed.patch(), eq(0));
        assert_that!(parsed.is_dev(), eq(true));
    }

    #[test]
    fn restate_version_serde() {
        let version = RestateVersion::new("1.66.2-dev".to_owned());
        let serialized = serde_json::to_string(&version).unwrap();
        assert_that!(serialized, eq(r#""1.66.2-dev""#));
        let deserialized: RestateVersion = serde_json::from_str(&serialized).unwrap();
        assert_that!(version, eq(deserialized));

        // same for semantic version
        let version = SemanticRestateVersion::from_str("1.66.2-dev").unwrap();
        let serialized = serde_json::to_string(&version).unwrap();
        println!("{serialized}");
        assert_that!(serialized, eq(r#""1.66.2-dev""#));
        let deserialized: SemanticRestateVersion = serde_json::from_str(&serialized).unwrap();
        assert_that!(version, eq(deserialized));
    }

    #[test]
    fn restate_version_newer_than() {
        // same same
        let version = SemanticRestateVersion::parse("1.66.2-dev").unwrap();
        assert_that!(version.is_equal_or_newer_than(&version), eq(true));

        let newer = SemanticRestateVersion::parse("1.66.2").unwrap();
        // both are equal
        assert_that!(newer.is_equal_or_newer_than(&version), eq(true));
        // 1.66.2 is newer than 1.66.2-dev
        assert_that!(version.is_equal_or_newer_than(&newer), eq(false));
        let even_newer = SemanticRestateVersion::parse("1.66.3").unwrap();
        assert_that!(even_newer.is_equal_or_newer_than(&newer), eq(true));
        assert_that!(newer.is_equal_or_newer_than(&even_newer), eq(false));

        let newer_dev = SemanticRestateVersion::parse("1.66.3-dev").unwrap();
        // 1.66.3-dev is newer than 1.66.2-dev
        assert_that!(newer_dev.is_equal_or_newer_than(&version), eq(true));

        let version = SemanticRestateVersion::parse("1.66.1").unwrap();
        let newer = SemanticRestateVersion::parse("1.66.2").unwrap();
        // newer is newer
        assert_that!(version.is_equal_or_newer_than(&newer), eq(false));
        assert_that!(newer.is_equal_or_newer_than(&version), eq(true));
    }
}
