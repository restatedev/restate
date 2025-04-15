// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bytestring::ByteString;

use restate_types::metadata::{GlobalMetadata, Precondition};

use crate::metadata_store::WriteError;

use super::MetadataWriter;

pub struct MetadataClientWrapper<'a> {
    writer: &'a MetadataWriter,
}

impl<'a> MetadataClientWrapper<'a> {
    pub fn new(writer: &'a MetadataWriter) -> Self {
        Self { writer }
    }

    /// Performs a put on metadata store for global metadata
    ///
    /// This method will update our local metadata view if the write was successful. It will also
    /// notify the metadata manager about the new version of the metadata if we saw a hint about
    /// it.
    pub async fn put<T>(&self, value: Arc<T>, precondition: Precondition) -> Result<(), WriteError>
    where
        T: GlobalMetadata,
    {
        match self
            .writer
            .metadata_store_client
            .put(
                ByteString::from_static(T::KEY),
                value.as_ref(),
                precondition.clone(),
            )
            .await
        {
            Ok(_) => {
                // todo: If we failed with precondition failure and we learned about a metadata version that we
                // don't have, we should inform metadata manager about it.
                let _ = self.writer.update(value.into_container()).await;
                Ok(())
            }
            Err(e @ WriteError::FailedPrecondition(_)) => {
                if let Precondition::MatchesVersion(v) = precondition {
                    self.writer
                        .inner
                        // we use v.next() because we don't get the "actual" version from the
                        // error type. A future improvement would be to let metadata client return
                        // the known conflicting version if possible.
                        .notify_observed_version(T::KIND, v.next(), None);
                }
                Err(e)
            }
            Err(e) => Err(e),
        }
    }
}
