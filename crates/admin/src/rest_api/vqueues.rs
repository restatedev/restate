// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::{Path, State};
use http::StatusCode;
use tracing::warn;

use restate_core::network::TransportConnect;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::VQueueId;
use restate_wal_protocol::{Command, Envelope, vqueues};

use super::create_envelope_header;
use super::error::*;
use crate::state::AdminServiceState;

/// Pauses a virtual queue.
#[utoipa::path(
    post,
    path = "/vqueues/{vqueue_id}/pause",
    operation_id = "pause_vqueue",
    tag = "vqueue",
    params(
        ("vqueue_id" = String, Path, description = "The virtual queue ID."),
    ),
    responses(
        (status = 202, description = "Virtual queue pause accepted and will be applied asynchronously"),
        MetaApiError
    )
)]
pub async fn pause_vqueue<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(mut state): State<
        AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>,
    >,
    Path(vqueue_id): Path<String>,
) -> Result<StatusCode, MetaApiError>
where
    Transport: TransportConnect,
{
    let vqueue_id = vqueue_id
        .parse::<VQueueId>()
        .map_err(|err| MetaApiError::InvalidField("vqueue_id", err.to_string()))?;
    let partition_key = vqueue_id.partition_key();
    let command = Command::VQueuesPause(
        vqueues::VQueuesPauseCommand {
            vqueues: vec![vqueue_id],
        }
        .bilrost_encode_to_bytes(),
    );

    ingest_vqueue_command(&mut state, partition_key, command).await
}

/// Resumes a virtual queue.
#[utoipa::path(
    post,
    path = "/vqueues/{vqueue_id}/resume",
    operation_id = "resume_vqueue",
    tag = "vqueue",
    params(
        ("vqueue_id" = String, Path, description = "The virtual queue ID."),
    ),
    responses(
        (status = 202, description = "Virtual queue resume accepted and will be applied asynchronously"),
        MetaApiError
    )
)]
pub async fn resume_vqueue<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(mut state): State<
        AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>,
    >,
    Path(vqueue_id): Path<String>,
) -> Result<StatusCode, MetaApiError>
where
    Transport: TransportConnect,
{
    let vqueue_id = vqueue_id
        .parse::<VQueueId>()
        .map_err(|err| MetaApiError::InvalidField("vqueue_id", err.to_string()))?;
    let partition_key = vqueue_id.partition_key();
    let command = Command::VQueuesResume(
        vqueues::VQueuesResumeCommand {
            vqueues: vec![vqueue_id],
        }
        .bilrost_encode_to_bytes(),
    );

    ingest_vqueue_command(&mut state, partition_key, command).await
}

async fn ingest_vqueue_command<Metadata, Discovery, Telemetry, Invocations, Transport>(
    state: &mut AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>,
    partition_key: PartitionKey,
    command: Command,
) -> Result<StatusCode, MetaApiError>
where
    Transport: TransportConnect,
{
    let envelope = Envelope::new(create_envelope_header(partition_key), command);

    let result = state
        .ingestion_client
        .ingest(partition_key, envelope)
        .await
        .map_err(|err| {
            warn!("Could not ingest virtual queue management command: {err}");
            MetaApiError::Internal(
                "Failed sending virtual queue management command to the cluster.".to_owned(),
            )
        })?;

    if let Err(err) = result.await {
        warn!("Could not ingest virtual queue management command: {err}");
        Err(MetaApiError::Internal(
            "Failed sending virtual queue management command to the cluster.".to_owned(),
        ))
    } else {
        Ok(StatusCode::ACCEPTED)
    }
}
