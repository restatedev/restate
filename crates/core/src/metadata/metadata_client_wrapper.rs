// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use bytestring::ByteString;
use tracing::debug;

use restate_metadata_store::{ReadError, ReadModifyWriteError, ReadWriteError, WriteError};
use restate_time_util::DurationExt;
use restate_types::Version;
use restate_types::config::Configuration;
use restate_types::errors::MaybeRetryableError;
use restate_types::metadata::{GlobalMetadata, Precondition};
use restate_types::net::metadata::Extraction;
use restate_types::retries::WaitDuration;

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
                precondition,
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

    /// Read Modify Write
    ///
    /// Performs a conditional modification of the metadata item. The write only succeeds if the stored value has not
    /// been modified in the meantime. If this should happen, then the read-modify-write cycle is internally retried.
    ///
    /// This method will update our local metadata view when it detects concurrent modifications and when it completes
    /// the write.
    ///
    /// On concurrent modification, the method will wait for the new version to be available before
    /// retrying but it tries to limit the wait time to the remaining time that's calculated from
    /// the cumulative time of the retry policy.
    pub async fn read_modify_write<T, F, E>(
        &self,
        mut modify: F,
    ) -> Result<Arc<T>, ReadModifyWriteError<E>>
    where
        T: GlobalMetadata + Extraction<Output = T> + 'static,
        F: FnMut(Option<Arc<T>>) -> Result<T, E>,
    {
        let mut retry_iter = Configuration::pinned()
            .common
            .metadata_client
            .backoff_policy
            .clone()
            .into_iter();

        let start_time = Instant::now();
        let total_retry_time = retry_iter.remaining_cumulative_duration();

        let key = ByteString::from_static(T::KEY);

        let mut retry_count = 0;
        loop {
            retry_count += 1;
            let old_value: Arc<T> = self.writer.inner.get(T::KIND);
            let old_value = (old_value.version() != Version::INVALID).then_some(old_value);

            let precondition = old_value
                .as_ref()
                .map(|c| Precondition::MatchesVersion(c.version()))
                .unwrap_or(Precondition::DoesNotExist);

            let result = modify(old_value);

            match result {
                Ok(new_value) => match self
                    .writer
                    .metadata_store_client
                    .put(key.clone(), &new_value, precondition)
                    .await
                {
                    Ok(()) => {
                        let new_value = Arc::new(new_value);
                        let _ = self.writer.update(new_value.clone().into_container()).await;
                        return Ok(new_value);
                    }

                    Err(WriteError::FailedPrecondition(err)) => {
                        let next_version = if let Precondition::MatchesVersion(v) = precondition {
                            // we use v.next() because we don't get the "actual" version from the
                            // error type. A future improvement would be to let metadata client return
                            // the known conflicting version if possible.
                            v.next()
                        } else {
                            Version::MIN
                        };
                        self.writer
                            .inner
                            .notify_observed_version(T::KIND, next_version, None);
                        debug!(
                            %retry_count,
                            "Concurrent value update: {err}"
                        );
                        let elapsed_time = start_time.elapsed();
                        let remaining = total_retry_time.subtract(elapsed_time);
                        // wait for this version to be fetched, but only wait up to the remaining
                        // time of the retry policy. If the remaining time is infinite, then we
                        // also wait indefinitely.
                        match remaining {
                            WaitDuration::Infinite => {
                                self.writer
                                    .inner
                                    .wait_for_version(T::KIND, next_version)
                                    .await
                                    .map_err(|e| {
                                        ReadModifyWriteError::ReadWrite(
                                            ReadError::terminal(e).into(),
                                        )
                                    })?;
                            }
                            WaitDuration::Finite(duration) => {
                                let _ = tokio::time::timeout(
                                    duration,
                                    self.writer.inner.wait_for_version(T::KIND, next_version),
                                )
                                .await
                                .map_err(|_elapsed| {
                                    // timed out, we'll report exhausted retries
                                    ReadWriteError::RetriesExhausted(key.clone())
                                })?
                                .map_err(|e| {
                                    // metadata manager stopped
                                    ReadModifyWriteError::ReadWrite(ReadError::terminal(e).into())
                                })?;
                            }
                            WaitDuration::None => {
                                return Err(ReadWriteError::RetriesExhausted(key).into());
                            }
                        }
                    }
                    Err(err) if err.retryable() => {
                        // calculates the minimum of remaining time and next retry delay.
                        if let Some(delay) = retry_iter.next() {
                            let elapsed_time = start_time.elapsed();
                            let remaining = total_retry_time.subtract(elapsed_time);
                            let Some(delay) = remaining.min(delay) else {
                                return Err(ReadWriteError::RetriesExhausted(key).into());
                            };
                            debug!(
                                %retry_count,
                                "Hit a retryable error: {err}; retrying in '{}'",
                                delay.friendly()
                            );
                            tokio::time::sleep(delay).await;
                        } else {
                            debug!(
                                %retry_count,
                                "Exhausted all retries"
                            );
                            return Err(ReadWriteError::RetriesExhausted(key).into());
                        }
                    }
                    Err(err) => {
                        return Err(ReadModifyWriteError::ReadWrite(err.into()));
                    }
                },
                Err(err) => return Err(ReadModifyWriteError::FailedOperation(err)),
            }
        }
    }
}
