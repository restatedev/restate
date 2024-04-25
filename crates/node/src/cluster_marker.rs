// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use once_cell::sync::Lazy;
use restate_types::config::node_filepath;
use semver::Version;
use std::cmp::{max_by, Ordering};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::ops::Deref;
use std::path::Path;
use tracing::info;

const CLUSTER_MARKER_FILE_NAME: &str = ".cluster-marker";
const TMP_CLUSTER_MARKER_FILE_NAME: &str = ".tmp-cluster-marker";

/// Map containing compatibility information which is used when validating the cluster marker. This
/// map only needs to contain the versions which change the compatibility with respect to the
/// previously registered versions.
///
/// # Important
/// This map needs to be updated whenever we release a version that is no longer compatible with
/// previous versions.
static COMPATIBILITY_MAP: Lazy<BTreeMap<Version, CompatibilityInformation>> = Lazy::new(|| {
    BTreeMap::from([(
        Version::new(0, 9, 0),
        CompatibilityInformation::new(Version::new(0, 9, 0)),
    )])
});

/// Compatibility information define the minimum supported Restate version that is compatible with
/// this version.
#[derive(Debug, Clone)]
struct CompatibilityInformation {
    min_supported_version: Version,
}

impl CompatibilityInformation {
    fn new(min_supported_version: Version) -> Self {
        Self {
            min_supported_version,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterValidationError {
    #[error("failed parsing restate version: {0}")]
    ParsingVersion(#[from] semver::Error),
    #[error("failed opening cluster marker file: {0}")]
    OpenFile(std::io::Error),
    #[error("failed writing new cluster marker file: {0}")]
    RenameFile(std::io::Error),
    #[error("failed decoding cluster marker: {0}")]
    Decode(serde_json::Error),
    #[error("failed encoding cluster marker: {0}")]
    Encode(serde_json::Error),
    #[error("trying to open data directory belonging to cluster '{persisted_cluster_name}' as cluster '{configured_cluster_name}'. Make sure that the right cluster accesses the data directory.")]
    IncorrectClusterName {
        configured_cluster_name: String,
        persisted_cluster_name: String,
    },
    #[error("current Restate version '{current_version}' is not compatible with data directory. Requiring Restate version >= '{min_version}'")]
    IncompatibleVersion {
        current_version: Version,
        min_version: Version,
    },
}

/// Marker which is stored in the Node's working directory telling about the
/// previous processes that worked on it before. It can be used for sanity checks.
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClusterMarker {
    cluster_name: String,
    max_version: Version,
    current_version: Version,
}

impl ClusterMarker {
    fn new(cluster_name: String, current_version: Version) -> Self {
        Self {
            cluster_name,
            max_version: current_version.clone(),
            current_version,
        }
    }
}

pub fn validate_and_update_cluster_marker(
    cluster_name: &str,
) -> Result<(), ClusterValidationError> {
    let current_version = Version::parse(env!("CARGO_PKG_VERSION"))?;
    let cluster_marker_filepath = node_filepath(CLUSTER_MARKER_FILE_NAME);

    validate_and_update_cluster_marker_inner(
        cluster_name,
        current_version,
        cluster_marker_filepath.as_path(),
        COMPATIBILITY_MAP.deref(),
    )
}

fn validate_and_update_cluster_marker_inner(
    cluster_name: &str,
    current_version: Version,
    cluster_marker_filepath: &Path,
    compatibility_map: &BTreeMap<Version, CompatibilityInformation>,
) -> Result<(), ClusterValidationError> {
    let tmp_cluster_marker_filepath = cluster_marker_filepath
        .parent()
        .expect("filepath should have parent directory")
        .join(TMP_CLUSTER_MARKER_FILE_NAME);
    let mut cluster_marker = if cluster_marker_filepath.exists() {
        let cluster_marker_file = std::fs::File::open(cluster_marker_filepath)
            .map_err(ClusterValidationError::OpenFile)?;
        serde_json::from_reader(&cluster_marker_file).map_err(ClusterValidationError::Decode)?
    } else {
        info!(
            "Did not find existing cluster marker. Creating a new one under '{}'.",
            cluster_marker_filepath.display()
        );
        ClusterMarker::new(cluster_name.to_owned(), current_version.clone())
    };

    // sanity checks
    if cluster_marker.cluster_name != cluster_name {
        return Err(ClusterValidationError::IncorrectClusterName {
            configured_cluster_name: cluster_name.to_owned(),
            persisted_cluster_name: cluster_marker.cluster_name,
        });
    }

    // find the next compatibility boundary wrt to the max version
    let (_, compatibility_boundary) = compatibility_map
        .range(..=cluster_marker.max_version.clone())
        .next_back()
        .expect("at least version 0.9.0 should be present");

    if current_version.cmp_precedence(&compatibility_boundary.min_supported_version)
        == Ordering::Less
    {
        return Err(ClusterValidationError::IncompatibleVersion {
            current_version,
            min_version: compatibility_boundary.min_supported_version.clone(),
        });
    }

    // update cluster marker
    cluster_marker.current_version = current_version.clone();
    cluster_marker.max_version = max_by(
        current_version,
        cluster_marker.max_version,
        Version::cmp_precedence,
    );

    // update cluster marker by writing to new file and then rename
    {
        // write the new cluster marker file
        let new_cluster_marker_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(tmp_cluster_marker_filepath.as_path())
            .map_err(ClusterValidationError::OpenFile)?;
        // using JSON encoding to be human-readable
        serde_json::to_writer(&new_cluster_marker_file, &cluster_marker)
            .map_err(ClusterValidationError::Encode)?;
    }

    std::fs::rename(
        tmp_cluster_marker_filepath.as_path(),
        cluster_marker_filepath,
    )
    .map_err(ClusterValidationError::RenameFile)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::cluster_marker::{
        validate_and_update_cluster_marker_inner, ClusterMarker, ClusterValidationError,
        CompatibilityInformation, CLUSTER_MARKER_FILE_NAME, COMPATIBILITY_MAP,
    };
    use once_cell::sync::Lazy;
    use semver::Version;
    use std::collections::BTreeMap;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::ops::Deref;
    use std::path::Path;
    use tempfile::{tempdir, NamedTempFile};

    fn read_cluster_marker(path: impl AsRef<Path>) -> anyhow::Result<ClusterMarker> {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(Into::into)
    }

    fn write_cluster_marker(
        cluster_marker: &ClusterMarker,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        serde_json::to_writer(&file, cluster_marker)?;
        file.flush()?;

        Ok(())
    }

    const CLUSTER_NAME: &str = "test";

    static TESTING_COMPATIBILITY_MAP: Lazy<BTreeMap<Version, CompatibilityInformation>> =
        Lazy::new(|| {
            BTreeMap::from([(
                Version::new(1, 0, 0),
                CompatibilityInformation::new(Version::new(1, 0, 0)),
            )])
        });

    #[test]
    fn cluster_marker_is_created() {
        let file = tempdir()
            .unwrap()
            .into_path()
            .join(CLUSTER_MARKER_FILE_NAME);
        let current_version = Version::new(1, 2, 3);

        validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            file.as_path(),
            TESTING_COMPATIBILITY_MAP.deref(),
        )
        .unwrap();

        let cluster_marker = read_cluster_marker(file.as_path()).unwrap();

        assert_eq!(
            cluster_marker,
            ClusterMarker {
                current_version: current_version.clone(),
                max_version: current_version,
                cluster_name: CLUSTER_NAME.to_owned(),
            }
        )
    }

    #[test]
    fn cluster_marker_is_updated() -> anyhow::Result<()> {
        let mut file = NamedTempFile::new().unwrap();
        let previous_version = Version::new(1, 1, 6);
        let current_version = Version::new(1, 2, 3);

        write_cluster_marker(
            &ClusterMarker::new(CLUSTER_NAME.to_owned(), previous_version),
            file.path(),
        )
        .unwrap();
        file.flush()?;

        validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            file.path(),
            TESTING_COMPATIBILITY_MAP.deref(),
        )
        .unwrap();

        let cluster_marker = read_cluster_marker(file.path()).unwrap();

        assert_eq!(
            cluster_marker,
            ClusterMarker {
                current_version: current_version.clone(),
                max_version: current_version,
                cluster_name: CLUSTER_NAME.to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn max_version_is_maintained() -> anyhow::Result<()> {
        let mut file = NamedTempFile::new().unwrap();
        let max_version = Version::new(1, 2, 6);
        let current_version = Version::new(1, 1, 3);

        write_cluster_marker(
            &ClusterMarker::new(CLUSTER_NAME.to_owned(), max_version.clone()),
            file.path(),
        )
        .unwrap();
        file.flush()?;

        validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            file.path(),
            TESTING_COMPATIBILITY_MAP.deref(),
        )
        .unwrap();

        let cluster_marker = read_cluster_marker(file.path()).unwrap();

        assert_eq!(
            cluster_marker,
            ClusterMarker {
                current_version: current_version.clone(),
                max_version,
                cluster_name: CLUSTER_NAME.to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn incompatible_cluster_name() -> anyhow::Result<()> {
        let mut file = NamedTempFile::new().unwrap();
        let max_version = Version::new(1, 2, 6);
        let current_version = Version::new(1, 1, 3);

        write_cluster_marker(
            &ClusterMarker::new("other_cluster".to_owned(), max_version.clone()),
            file.path(),
        )
        .unwrap();
        file.flush()?;

        let result = validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            file.path(),
            TESTING_COMPATIBILITY_MAP.deref(),
        );
        assert!(matches!(
            result,
            Err(ClusterValidationError::IncorrectClusterName { .. })
        ));
        Ok(())
    }

    #[test]
    fn incompatible_version() -> anyhow::Result<()> {
        let mut file = NamedTempFile::new().unwrap();
        let max_version = Version::new(1, 2, 6);
        let compatibility_boundary = Version::new(1, 1, 1);
        let current_version = Version::new(1, 0, 3);

        write_cluster_marker(
            &ClusterMarker::new(CLUSTER_NAME.to_owned(), max_version.clone()),
            file.path(),
        )
        .unwrap();
        file.flush()?;

        let mut compatibility_map = COMPATIBILITY_MAP.deref().clone();
        compatibility_map.insert(
            compatibility_boundary.clone(),
            CompatibilityInformation::new(compatibility_boundary),
        );

        let result = validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            file.path(),
            &compatibility_map,
        );
        assert!(matches!(
            result,
            Err(ClusterValidationError::IncompatibleVersion { .. })
        ));
        Ok(())
    }
}
