// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::fs::{File, OpenOptions};
use std::path::Path;
use tracing::debug;

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
    CompatibilityInformation::new(Version::new(1, 4, 0), Version::new(1, 0, 0));

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
    #[error("failed syncing the cluster marker file: {0}")]
    SyncFile(std::io::Error),
    #[error("failed writing new cluster marker file: {0}")]
    RenameFile(std::io::Error),
    #[error("failed decoding cluster marker: {0}")]
    Decode(serde_json::Error),
    #[error("failed encoding cluster marker: {0}")]
    Encode(serde_json::Error),
    #[error(
        "trying to open data directory belonging to cluster '{persisted_cluster_name}' as cluster '{configured_cluster_name}'. Make sure that the right cluster accesses the data directory."
    )]
    IncorrectClusterName {
        configured_cluster_name: String,
        persisted_cluster_name: String,
    },
    #[error(
        "Restate version '{this_version}' is forward incompatible with data directory. Requiring Restate version >= '{min_version}'"
    )]
    ForwardIncompatibility {
        this_version: Version,
        min_version: Version,
    },
    #[error(
        "Restate version '{this_version}' is backward incompatible with data directory created by Restate version '{data_version}'"
    )]
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
    /// True if the cluster is known to be provisioned. Versions < 1.2 don't have this field stored.
    is_provisioned: Option<bool>,
}

impl ClusterMarker {
    fn new(
        cluster_name: String,
        current_version: Version,
        min_forward_compatible_version: Version,
        is_provisioned: bool,
    ) -> Self {
        Self {
            cluster_name,
            max_version: current_version.clone(),
            current_version,
            min_forward_compatible_version: Some(min_forward_compatible_version),
            is_provisioned: Some(is_provisioned),
        }
    }
}

/// Validates and updates the cluster marker wrt to the currently used Restate version. Returns
/// whether the cluster was provisioned before.
pub fn validate_and_update_cluster_marker(
    cluster_name: &str,
) -> Result<bool, ClusterValidationError> {
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
) -> Result<bool, ClusterValidationError> {
    let mut cluster_marker = if cluster_marker_filepath.exists() {
        read_cluster_marker(cluster_marker_filepath)?
    } else {
        debug!(
            "Did not find existing cluster marker. Creating a new one under '{}'.",
            cluster_marker_filepath.display()
        );
        ClusterMarker::new(
            cluster_name.to_owned(),
            this_version.clone(),
            compatibility_information
                .min_forward_compatible_version
                .clone(),
            false,
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

    write_new_cluster_marker(cluster_marker_filepath, &cluster_marker)?;

    Ok(cluster_marker.is_provisioned.unwrap_or_default())
}

fn write_new_cluster_marker(
    cluster_marker_filepath: &Path,
    new_cluster_marker: &ClusterMarker,
) -> Result<(), ClusterValidationError> {
    let tmp_cluster_marker_filepath = cluster_marker_filepath
        .parent()
        .expect("filepath should have parent directory")
        .join(TMP_CLUSTER_MARKER_FILE_NAME);

    // update cluster marker by writing to a new file and then rename it
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
        serde_json::to_writer(&new_cluster_marker_file, &new_cluster_marker)
            .map_err(ClusterValidationError::Encode)?;

        // make sure the new cluster marker file is persisted
        new_cluster_marker_file
            .sync_all()
            .map_err(ClusterValidationError::SyncFile)?;
    }

    // atomically rename the new cluster marker file into the old cluster marker file
    std::fs::rename(
        tmp_cluster_marker_filepath.as_path(),
        cluster_marker_filepath,
    )
    .map_err(ClusterValidationError::RenameFile)?;

    // make sure the rename operation is persisted to disk by flushing the parent directory
    // Note: On Windows, you cannot open a directory as a file to sync it, so we skip this step.
    // Windows filesystem semantics handle rename durability differently.
    #[cfg(unix)]
    {
        let parent = cluster_marker_filepath
            .parent()
            .expect("cluster marker file to be not the root");
        let parent_dir = File::open(parent).expect("to open parent directory");
        parent_dir
            .sync_all()
            .map_err(ClusterValidationError::SyncFile)?;
    }
    Ok(())
}

fn read_cluster_marker(
    cluster_marker_filepath: &Path,
) -> Result<ClusterMarker, ClusterValidationError> {
    let cluster_marker_file =
        File::open(cluster_marker_filepath).map_err(ClusterValidationError::CreateFile)?;
    serde_json::from_reader::<_, ClusterMarker>(&cluster_marker_file)
        .map_err(ClusterValidationError::Decode)
}

/// Marks the cluster as provisioned in the cluster marker
pub fn mark_cluster_as_provisioned() -> Result<(), ClusterValidationError> {
    let cluster_marker_filepath = node_filepath(CLUSTER_MARKER_FILE_NAME);
    mark_cluster_as_provisioned_inner(cluster_marker_filepath.as_path())
}

fn mark_cluster_as_provisioned_inner(
    cluster_marker_filepath: &Path,
) -> Result<(), ClusterValidationError> {
    let mut cluster_marker = read_cluster_marker(cluster_marker_filepath)?;
    cluster_marker.is_provisioned = Some(true);
    write_new_cluster_marker(cluster_marker_filepath, &cluster_marker)
}

#[cfg(test)]
mod tests {
    use crate::cluster_marker::{
        CLUSTER_MARKER_FILE_NAME, COMPATIBILITY_INFORMATION, ClusterMarker, ClusterValidationError,
        CompatibilityInformation, mark_cluster_as_provisioned_inner,
        validate_and_update_cluster_marker_inner,
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
                ),
                is_provisioned: Some(false),
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
                true,
            ),
            &file,
        )?;

        validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            &file,
            &TESTING_COMPATIBILITY_INFORMATION,
        )?;

        let cluster_marker = read_cluster_marker(file)?;

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
                ),
                is_provisioned: Some(true),
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
                false,
            ),
            &file,
        )?;

        validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            current_version.clone(),
            &file,
            &TESTING_COMPATIBILITY_INFORMATION,
        )?;

        let cluster_marker = read_cluster_marker(file)?;

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
                is_provisioned: Some(false),
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
                true,
            ),
            &file,
        )?;

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
        let dir = tempdir()?;
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let max_version = Version::new(2, 2, 6);
        let this_version = Version::new(1, 0, 3);

        write_cluster_marker(
            &ClusterMarker::new(
                CLUSTER_NAME.to_owned(),
                max_version.clone(),
                Version::new(2, 0, 0),
                true,
            ),
            &file,
        )?;

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
                true,
            ),
            &file,
        )?;

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
        let dir = tempdir()?;
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
                false,
            ),
            &file,
        )?;

        let result = validate_and_update_cluster_marker_inner(
            CLUSTER_NAME,
            this_version.clone(),
            &file,
            &TESTING_COMPATIBILITY_INFORMATION,
        );
        assert!(result.is_ok());

        Ok(())
    }

    #[test]
    fn mark_cluster_marker_as_provisioned() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join(CLUSTER_MARKER_FILE_NAME);
        let version = Version::new(2, 2, 6);

        write_cluster_marker(
            &ClusterMarker::new(
                CLUSTER_NAME.to_owned(),
                version.clone(),
                TESTING_COMPATIBILITY_INFORMATION
                    .min_forward_compatible_version
                    .clone(),
                false,
            ),
            &file,
        )?;

        mark_cluster_as_provisioned_inner(&file)?;

        let cluster_marker = read_cluster_marker(file)?;

        assert_eq!(
            cluster_marker,
            ClusterMarker {
                current_version: version.clone(),
                max_version: version,
                cluster_name: CLUSTER_NAME.to_owned(),
                min_forward_compatible_version: Some(
                    TESTING_COMPATIBILITY_INFORMATION
                        .min_forward_compatible_version
                        .clone()
                ),
                is_provisioned: Some(true),
            }
        );

        Ok(())
    }
}
