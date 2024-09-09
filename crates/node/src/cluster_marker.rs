// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::config::node_filepath;
use semver::Version;
use std::cmp::Ordering;
use std::fs::OpenOptions;
use std::path::Path;
use tracing::info;

const CLUSTER_MARKER_FILE_NAME: &str = ".cluster-marker";
const TMP_CLUSTER_MARKER_FILE_NAME: &str = ".tmp-cluster-marker";

/// Compatibility information for this version.
///
/// It includes:
/// * Minimum forward compatible version which can still read data written by this version
/// * Minimum backward compatible version whose data this version can still read
///
/// # Important
/// This information needs to be updated whenever we release a version that changes the
/// compatible versions boundaries.
static COMPATIBILITY_INFORMATION: CompatibilityInformation =
    CompatibilityInformation::new(Version::new(1, 0, 0), Version::new(1, 0, 0));

/// Compatibility information defining the minimum Restate version that can read data written by
/// this version. Additionally, it specifies the minimum with which this version is backwards
/// compatible.
#[derive(Debug, Clone)]
struct CompatibilityInformation {
    /// Minimum version required to read data written by this version.
    min_forward_compatible_version: Version,
    /// Minimum version from which this version can read data.
    min_backward_compatible_version: Version,
}

impl CompatibilityInformation {
    const fn new(
        min_forward_compatible_version: Version,
        min_backward_compatible_version: Version,
    ) -> Self {
        Self {
            min_forward_compatible_version,
            min_backward_compatible_version,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterValidationError {
    #[error("failed parsing restate version: {0}")]
    ParsingVersion(#[from] semver::Error),
    #[error("failed creating cluster marker file: {0}")]
    CreateFile(std::io::Error),
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
    #[error("Restate version '{this_version}' is forward incompatible with data directory. Requiring Restate version >= '{min_version}'")]
    ForwardIncompatibility {
        this_version: Version,
        min_version: Version,
    },
    #[error("Restate version '{this_version}' is backward incompatible with data directory created by Restate version '{data_version}'")]
    BackwardIncompatibility {
        this_version: Version,
        data_version: Version,
    },
}

/// Marker stored in the Node's working directory with metadata about the cluster it belongs to and
/// compatible software versions.
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ClusterMarker {
    cluster_name: String,
    /// The highest version that has operated on the data directory.
    max_version: Version,
    /// The most recent version to operate on the data directory.
    current_version: Version,
    /// Minimum required version to read data. Optional since it was introduced after 0.9.
    /// This field should only be updated when updating the `max_version` field.
    min_forward_compatible_version: Option<Version>,
}

impl ClusterMarker {
    fn new(
        cluster_name: String,
        current_version: Version,
        min_forward_compatible_version: Version,
    ) -> Self {
        Self {
            cluster_name,
            max_version: current_version.clone(),
            current_version,
            min_forward_compatible_version: Some(min_forward_compatible_version),
        }
    }
}

/// Validates and updates the cluster marker wrt to the currently used Restate version.
pub fn validate_and_update_cluster_marker(
    cluster_name: &str,
) -> Result<(), ClusterValidationError> {
    let this_version = Version::parse(env!("CARGO_PKG_VERSION"))?;
    let cluster_marker_filepath = node_filepath(CLUSTER_MARKER_FILE_NAME);

    validate_and_update_cluster_marker_inner(
        cluster_name,
        this_version,
        cluster_marker_filepath.as_path(),
        &COMPATIBILITY_INFORMATION,
    )
}

fn validate_and_update_cluster_marker_inner(
    cluster_name: &str,
    this_version: Version,
    cluster_marker_filepath: &Path,
    compatibility_information: &CompatibilityInformation,
) -> Result<(), ClusterValidationError> {
    let tmp_cluster_marker_filepath = cluster_marker_filepath
        .parent()
        .expect("filepath should have parent directory")
        .join(TMP_CLUSTER_MARKER_FILE_NAME);
    let mut cluster_marker = if cluster_marker_filepath.exists() {
        let cluster_marker_file = std::fs::File::open(cluster_marker_filepath)
            .map_err(ClusterValidationError::CreateFile)?;
        serde_json::from_reader(&cluster_marker_file).map_err(ClusterValidationError::Decode)?
    } else {
        info!(
            "Did not find existing cluster marker. Creating a new one under '{}'.",
            cluster_marker_filepath.display()
        );
        ClusterMarker::new(
            cluster_name.to_owned(),
            this_version.clone(),
            compatibility_information
                .min_forward_compatible_version
                .clone(),
        )
    };

    // sanity checks
    if cluster_marker.cluster_name != cluster_name {
        return Err(ClusterValidationError::IncorrectClusterName {
            configured_cluster_name: cluster_name.to_owned(),
            persisted_cluster_name: cluster_marker.cluster_name,
        });
    }

    // versions 0.9 and 1.0.0 don't write this field --> default to current version
    let min_forward_compatible_version = cluster_marker
        .min_forward_compatible_version
        .clone()
        .unwrap_or(cluster_marker.current_version.clone());

    // The data directory required minimum version must be compatible with the running version.
    // Asserts that: this_version >= cluster_marker.min_forward_compatible_version
    if this_version.cmp_precedence(&min_forward_compatible_version) == Ordering::Less {
        return Err(ClusterValidationError::ForwardIncompatibility {
            this_version,
            min_version: min_forward_compatible_version,
        });
    }

    // The running version must be backwards-compatible with the version of the data directory.
    // Asserts that: cluster_marker.current_version >= min_backward_compatible_version
    if cluster_marker
        .current_version
        .cmp_precedence(&compatibility_information.min_backward_compatible_version)
        == Ordering::Less
    {
        return Err(ClusterValidationError::BackwardIncompatibility {
            this_version,
            data_version: cluster_marker.current_version,
        });
    }

    // update cluster marker
    cluster_marker.current_version = this_version.clone();

    if this_version.cmp_precedence(&cluster_marker.max_version) == Ordering::Greater {
        cluster_marker.max_version = this_version;
        cluster_marker.min_forward_compatible_version = Some(
            compatibility_information
                .min_forward_compatible_version
                .clone(),
        );
    }

    // update cluster marker by writing to new file and then rename
    {
        // create parent directories if not present
        if let Some(parent) = tmp_cluster_marker_filepath.parent() {
            std::fs::create_dir_all(parent).map_err(ClusterValidationError::CreateFile)?;
        }

        // write the new cluster marker file
        let new_cluster_marker_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(tmp_cluster_marker_filepath.as_path())
            .map_err(ClusterValidationError::CreateFile)?;
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
        CompatibilityInformation, CLUSTER_MARKER_FILE_NAME, COMPATIBILITY_INFORMATION,
    };
    use semver::Version;
    use std::fs;
    use std::fs::OpenOptions;
    use std::path::Path;
    use tempfile::tempdir;

    fn read_cluster_marker(path: impl AsRef<Path>) -> anyhow::Result<ClusterMarker> {
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(Into::into)
    }

    fn write_cluster_marker(
        cluster_marker: &ClusterMarker,
        path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;
        serde_json::to_writer(&file, cluster_marker)?;
        Ok(())
    }

    const CLUSTER_NAME: &str = "test";

    static TESTING_COMPATIBILITY_INFORMATION: CompatibilityInformation =
        CompatibilityInformation::new(Version::new(2, 0, 0), Version::new(1, 0, 0));

    #[test]
    fn cluster_marker_is_created() {
        let dir = tempdir().unwrap();
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let current_version = Version::new(2, 2, 3);

        validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            file.as_path(),
            &TESTING_COMPATIBILITY_INFORMATION,
        )
        .unwrap();

        let cluster_marker = read_cluster_marker(file.as_path()).unwrap();

        assert_eq!(
            cluster_marker,
            ClusterMarker {
                current_version: current_version.clone(),
                max_version: current_version,
                cluster_name: CLUSTER_NAME.to_owned(),
                min_forward_compatible_version: Some(
                    TESTING_COMPATIBILITY_INFORMATION
                        .min_forward_compatible_version
                        .clone()
                )
            }
        )
    }

    #[test]
    fn cluster_marker_is_updated() -> anyhow::Result<()> {
        let dir = tempdir().unwrap();
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let previous_version = Version::new(1, 1, 6);
        let current_version = Version::new(2, 2, 3);

        write_cluster_marker(
            &ClusterMarker::new(
                CLUSTER_NAME.to_owned(),
                previous_version,
                TESTING_COMPATIBILITY_INFORMATION
                    .min_forward_compatible_version
                    .clone(),
            ),
            &file,
        )
        .unwrap();

        validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            &file,
            &TESTING_COMPATIBILITY_INFORMATION,
        )
        .unwrap();

        let cluster_marker = read_cluster_marker(file).unwrap();

        assert_eq!(
            cluster_marker,
            ClusterMarker {
                current_version: current_version.clone(),
                max_version: current_version,
                cluster_name: CLUSTER_NAME.to_owned(),
                min_forward_compatible_version: Some(
                    TESTING_COMPATIBILITY_INFORMATION
                        .min_forward_compatible_version
                        .clone()
                )
            }
        );
        Ok(())
    }

    #[test]
    fn max_version_is_maintained() -> anyhow::Result<()> {
        let dir = tempdir().unwrap();
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let max_version = Version::new(2, 2, 6);
        let current_version = Version::new(2, 1, 3);

        write_cluster_marker(
            &ClusterMarker::new(
                CLUSTER_NAME.to_owned(),
                max_version.clone(),
                TESTING_COMPATIBILITY_INFORMATION
                    .min_forward_compatible_version
                    .clone(),
            ),
            &file,
        )
        .unwrap();

        validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            &file,
            &TESTING_COMPATIBILITY_INFORMATION,
        )
        .unwrap();

        let cluster_marker = read_cluster_marker(file).unwrap();

        assert_eq!(
            cluster_marker,
            ClusterMarker {
                current_version: current_version.clone(),
                max_version,
                cluster_name: CLUSTER_NAME.to_owned(),
                min_forward_compatible_version: Some(
                    TESTING_COMPATIBILITY_INFORMATION
                        .min_forward_compatible_version
                        .clone()
                ),
            }
        );
        Ok(())
    }

    #[test]
    fn incompatible_cluster_name() -> anyhow::Result<()> {
        let dir = tempdir().unwrap();
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let max_version = Version::new(2, 2, 6);
        let current_version = Version::new(2, 1, 3);

        write_cluster_marker(
            &ClusterMarker::new(
                "other_cluster".to_owned(),
                max_version.clone(),
                TESTING_COMPATIBILITY_INFORMATION
                    .min_forward_compatible_version
                    .clone(),
            ),
            &file,
        )
        .unwrap();

        let result = validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            &file,
            &TESTING_COMPATIBILITY_INFORMATION,
        );
        assert!(matches!(
            result,
            Err(ClusterValidationError::IncorrectClusterName { .. })
        ));
        Ok(())
    }

    #[test]
    fn forward_incompatible_version() -> anyhow::Result<()> {
        let dir = tempdir().unwrap();
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let max_version = Version::new(2, 2, 6);
        let this_version = Version::new(1, 0, 3);

        write_cluster_marker(
            &ClusterMarker::new(
                CLUSTER_NAME.to_owned(),
                max_version.clone(),
                Version::new(2, 0, 0),
            ),
            &file,
        )
        .unwrap();

        let result = validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            this_version.clone(),
            &file,
            &COMPATIBILITY_INFORMATION,
        );
        assert!(matches!(
            result,
            Err(ClusterValidationError::ForwardIncompatibility { .. })
        ));
        Ok(())
    }

    #[test]
    fn backward_incompatible_version() -> anyhow::Result<()> {
        let dir = tempdir().unwrap();
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let max_version = Version::new(0, 9, 2);
        let this_version = Version::new(2, 1, 1);
        write_cluster_marker(
            &ClusterMarker::new(
                CLUSTER_NAME.to_owned(),
                max_version.clone(),
                Version::new(0, 9, 0),
            ),
            &file,
        )
        .unwrap();

        let result = validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            this_version.clone(),
            &file,
            &COMPATIBILITY_INFORMATION,
        );
        assert!(matches!(
            result,
            Err(ClusterValidationError::BackwardIncompatibility { .. })
        ));

        Ok(())
    }

    #[test]
    fn compatible_version() -> anyhow::Result<()> {
        let dir = tempdir().unwrap();
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let max_version = Version::new(2, 0, 2);
        let this_version = Version::new(2, 3, 1);
        write_cluster_marker(
            &ClusterMarker::new(
                CLUSTER_NAME.to_owned(),
                max_version.clone(),
                TESTING_COMPATIBILITY_INFORMATION
                    .min_forward_compatible_version
                    .clone(),
            ),
            &file,
        )
        .unwrap();

        let result = validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            this_version.clone(),
            &file,
            &TESTING_COMPATIBILITY_INFORMATION,
        );
        assert!(result.is_ok());

        Ok(())
    }
}
