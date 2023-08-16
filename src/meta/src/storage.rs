// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::future::BoxFuture;
use futures::FutureExt;
use restate_schema_impl::SchemasUpdateCommand;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io;
use tracing::trace;

#[derive(Debug, thiserror::Error)]
pub enum MetaStorageError {
    #[error("generic io error: {0}")]
    Io(#[from] io::Error),
    #[error("generic serde error: {0}. This is probably a runtime bug")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("generic serde error: {0}. This is probably a runtime bug")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("generic descriptor error: {0}. This is probably a runtime bug")]
    Descriptor(#[from] prost_reflect::DescriptorError),
    #[error("task error when writing to disk: {0}. This is probably a runtime bug")]
    Join(#[from] tokio::task::JoinError),
    #[error("file ending with .restate has a bad filename: {0}. This is probably a runtime bug")]
    BadFilename(PathBuf),
}

pub trait MetaStorage {
    // TODO: Replace with async trait or proper future
    fn store(
        &mut self,
        commands: Vec<SchemasUpdateCommand>,
    ) -> BoxFuture<Result<(), MetaStorageError>>;

    // TODO: Replace with async trait or proper future
    fn reload(&mut self) -> BoxFuture<Result<Vec<SchemasUpdateCommand>, MetaStorageError>>;
}

// --- File based implementation of MetaStorage, using bincode

const RESTATE_EXTENSION: &str = "restate";

#[derive(Debug)]
pub struct FileMetaStorage {
    root_path: PathBuf,
    next_file_index: usize,
}

impl FileMetaStorage {
    pub fn new(root_path: PathBuf) -> Self {
        Self {
            root_path,
            next_file_index: 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct CommandsFile(Vec<SchemasUpdateCommand>);

impl MetaStorage for FileMetaStorage {
    fn store(
        &mut self,
        commands: Vec<SchemasUpdateCommand>,
    ) -> BoxFuture<Result<(), MetaStorageError>> {
        let file_path = self
            .root_path
            .join(format!("{}.{}", self.next_file_index, RESTATE_EXTENSION));
        self.next_file_index += 1;

        trace!("Write metadata file {}", file_path.display());

        // We use blocking spawn to use bincode::encode_into_std_write
        async {
            tokio::task::spawn_blocking(move || {
                let mut file = std::fs::File::create(file_path)?;
                bincode::serde::encode_into_std_write(
                    CommandsFile(commands),
                    &mut file,
                    bincode::config::standard(),
                )?;
                Result::<(), MetaStorageError>::Ok(file.sync_all()?)
            })
            .await??;
            Ok(())
        }
        .boxed()
    }

    fn reload(&mut self) -> BoxFuture<Result<Vec<SchemasUpdateCommand>, MetaStorageError>> {
        let root_path = self.root_path.clone();

        async {
            // Try to create a dir, in case it doesn't exist
            restate_fs_util::create_dir_all_if_doesnt_exists(&root_path).await?;

            // Find all the metadata files in the root path directory, parse the index and then sort them by index
            let mut read_dir = tokio::fs::read_dir(root_path).await?;
            let mut metadata_files = vec![];
            while let Some(dir_entry) = read_dir.next_entry().await? {
                if dir_entry
                    .path()
                    .extension()
                    .and_then(|os_str| os_str.to_str())
                    == Some(RESTATE_EXTENSION)
                {
                    let index: usize = dir_entry
                        .path()
                        .file_stem()
                        .expect("If there is an extension, there must be a file stem")
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| MetaStorageError::BadFilename(dir_entry.path()))?;

                    // Make sure self.next_file_index = max(self.next_file_index, index + 1)
                    self.next_file_index = self.next_file_index.max(index + 1);
                    metadata_files.push((dir_entry.path(), index));
                }
            }
            metadata_files.sort_by(|a, b| a.1.cmp(&b.1));

            // We use blocking spawn to use bincode::decode_from_std_read
            tokio::task::spawn_blocking(move || {
                let mut schemas_updates = vec![];

                for (metadata_file_path, _) in metadata_files {
                    // Metadata_file_path is the json metadata descriptor
                    trace!("Reloading metadata file {}", metadata_file_path.display());

                    let mut file = std::fs::File::open(metadata_file_path)?;

                    let commands_file: CommandsFile = bincode::serde::decode_from_std_read(
                        &mut file,
                        bincode::config::standard(),
                    )?;
                    schemas_updates.extend(commands_file.0);
                }

                Result::<Vec<SchemasUpdateCommand>, MetaStorageError>::Ok(schemas_updates)
            })
            .await?
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_pb::mocks;
    use restate_schema_api::endpoint::EndpointMetadata;
    use restate_schema_api::key::ServiceInstanceType;
    use restate_schema_impl::{Schemas, ServiceRegistrationRequest};
    use restate_test_util::test;
    use tempfile::tempdir;

    #[test(tokio::test)]
    async fn reload_in_order() {
        let schemas = Schemas::default();
        let temp_dir = tempdir().unwrap();
        let mut file_storage = FileMetaStorage::new(temp_dir.path().to_path_buf());

        // Generate some commands for a new endpoint, with new services
        let commands_1 = schemas
            .compute_new_endpoint_updates(
                EndpointMetadata::mock_with_uri("http://localhost:8080"),
                vec![ServiceRegistrationRequest::new(
                    mocks::GREETER_SERVICE_NAME.to_string(),
                    ServiceInstanceType::Unkeyed,
                )],
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            )
            .unwrap();

        file_storage.store(commands_1.clone()).await.unwrap();

        // Generate some commands for a new endpoint, with a new and old service
        // We need to apply updates to generate a new command list
        schemas.apply_updates(commands_1.clone()).unwrap();
        let commands_2 = schemas
            .compute_new_endpoint_updates(
                EndpointMetadata::mock_with_uri("http://localhost:8081"),
                vec![
                    ServiceRegistrationRequest::new(
                        mocks::GREETER_SERVICE_NAME.to_string(),
                        ServiceInstanceType::Unkeyed,
                    ),
                    ServiceRegistrationRequest::new(
                        mocks::ANOTHER_GREETER_SERVICE_NAME.to_string(),
                        ServiceInstanceType::Unkeyed,
                    ),
                ],
                mocks::DESCRIPTOR_POOL.clone(),
                false,
            )
            .unwrap();

        file_storage.store(commands_2.clone()).await.unwrap();

        // Check we can apply these commands
        schemas.apply_updates(commands_2.clone()).unwrap();

        let mut expected_commands = vec![];
        expected_commands.extend(commands_1);
        expected_commands.extend(commands_2);
        let expected_commands: Vec<SchemasUpdateCommandEquality> =
            expected_commands.into_iter().map(Into::into).collect();

        // Now let's try to reload
        let mut file_storage = FileMetaStorage::new(temp_dir.path().to_path_buf());
        let actual_commands = file_storage.reload().await.unwrap();

        assert_eq!(
            actual_commands
                .into_iter()
                .map(SchemasUpdateCommandEquality::from)
                .collect::<Vec<_>>(),
            expected_commands
        );
    }

    // Newtype to implement equality for the scope of this test
    #[derive(Debug)]
    struct SchemasUpdateCommandEquality(SchemasUpdateCommand);

    impl From<SchemasUpdateCommand> for SchemasUpdateCommandEquality {
        fn from(value: SchemasUpdateCommand) -> Self {
            Self(value)
        }
    }

    impl PartialEq for SchemasUpdateCommandEquality {
        fn eq(&self, other: &Self) -> bool {
            match (&self.0, &other.0) {
                (
                    SchemasUpdateCommand::InsertEndpoint {
                        metadata: self_metadata,
                        services: self_services,
                        ..
                    },
                    SchemasUpdateCommand::InsertEndpoint {
                        metadata: other_metadata,
                        services: other_services,
                        ..
                    },
                ) => self_metadata.id() == other_metadata.id() && self_services == other_services,
                (
                    SchemasUpdateCommand::RemoveService {
                        name: self_name,
                        revision: self_revision,
                    },
                    SchemasUpdateCommand::RemoveService {
                        name: other_name,
                        revision: other_revision,
                    },
                ) => self_name == other_name && self_revision == other_revision,
                _ => false,
            }
        }
    }

    impl Eq for SchemasUpdateCommandEquality {}
}
