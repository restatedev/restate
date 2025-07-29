// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod appender;
mod background_appender;
mod bifrost;
mod bifrost_admin;
mod error;
mod log_chain_writer;
pub mod loglet;
mod loglet_wrapper;
pub mod providers;
mod read_stream;
mod record;
mod sealed_loglet;
mod service;
mod types;
mod watchdog;

pub use appender::Appender;
pub use background_appender::{AppenderHandle, BackgroundAppender, CommitToken, LogSender};
pub use bifrost::{Bifrost, ErrorRecoveryStrategy};
pub use bifrost_admin::{BifrostAdmin, MaybeSealedSegment};
pub use error::{Error, Result};
pub use read_stream::LogReadStream;
pub use record::{InputRecord, LogEntry};
pub use service::BifrostService;
pub use types::*;

use std::sync::Arc;

use restate_core::{Metadata, ShutdownError};
use restate_types::Version;
use restate_types::identifiers::WithPartitionKey;
use restate_types::logs::{LogId, Lsn};
use restate_types::partition_table::{FindPartition, PartitionTableError};

pub const SMALL_BATCH_THRESHOLD_COUNT: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error("partition not found: {0}")]
    PartitionNotFound(#[from] PartitionTableError),
    #[error(transparent)]
    Bifrost(#[from] Error),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

/// Appends the given envelope to the provided Bifrost instance. The log instance is chosen
/// based on the envelopes partition key.
///
/// Important: This method must only be called in the context of a [`TaskCenter`] task because
/// it needs access to [`metadata()`].
///
/// todo: This method should be removed in favor of using Appender/BackgroundAppender API in
/// Bifrost. Additionally, the check for partition_table is probably unnecessary in the vast
/// majority of call-sites.
pub async fn append_to_bifrost<T>(
    bifrost: &Bifrost,
    envelope: Arc<T>,
) -> Result<(LogId, Lsn), AppendError>
where
    T: WithPartitionKey
        + restate_types::storage::StorageEncode
        + restate_types::logs::HasRecordKeys,
{
    let partition_id = {
        // make sure we drop pinned partition table before awaiting
        let partition_table = Metadata::current()
            .wait_for_partition_table(Version::MIN)
            .await?;
        partition_table.find_partition_id(envelope.partition_key())?
    };

    let log_id = LogId::from(*partition_id);
    // todo: Pass the envelope as `Arc` to `append_envelope_to_bifrost` instead. Possibly use
    // triomphe's UniqueArc for a mutable Arc during construction.
    let lsn = bifrost
        .append(log_id, ErrorRecoveryStrategy::default(), envelope)
        .await?;

    Ok((log_id, lsn))
}
