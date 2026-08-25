// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod describe;
mod list;
mod pause;
mod resume;

use chrono::{DateTime, Local};
use cling::prelude::*;
use restate_types::vqueues::VQueueId;
use serde::Deserialize;

use crate::clients::DataFusionHttpClient;

const VQUEUE_COLUMNS: &str = "id, is_active, queue_is_paused, service_name, scope, limit_key, \
    lock_name, created_at, last_enqueued_at, last_start_at, last_attempt_at, last_finish_at, \
    num_inbox, num_running, num_suspended, num_paused, num_finished";

#[derive(Run, Subcommand, Clone)]
pub enum VQueues {
    /// List virtual queues
    List(list::List),
    /// Print detailed information about a virtual queue
    Describe(describe::Describe),
    /// Pause a virtual queue
    #[command(hide = true)]
    Pause(pause::Pause),
    /// Resume a virtual queue
    #[command(hide = true)]
    Resume(resume::Resume),
}

#[derive(Debug, Clone, Deserialize)]
struct VQueueRow {
    id: String,
    is_active: bool,
    queue_is_paused: bool,
    service_name: Option<String>,
    scope: Option<String>,
    limit_key: Option<String>,
    lock_name: Option<String>,
    created_at: DateTime<Local>,
    last_enqueued_at: Option<DateTime<Local>>,
    last_start_at: Option<DateTime<Local>>,
    last_attempt_at: Option<DateTime<Local>>,
    last_finish_at: Option<DateTime<Local>>,
    num_inbox: u64,
    num_running: u64,
    num_suspended: u64,
    num_paused: u64,
    num_finished: u64,
}

async fn get_vqueue(
    client: &DataFusionHttpClient,
    vqueue_id: &VQueueId,
) -> anyhow::Result<VQueueRow> {
    let mut rows: Vec<VQueueRow> = client
        .run_json_query(format!(
            "SELECT {VQUEUE_COLUMNS} FROM sys_vqueue_meta WHERE id = '{vqueue_id}'"
        ))
        .await?;
    rows.pop()
        .ok_or_else(|| anyhow::anyhow!("Virtual queue {vqueue_id} not found!"))
}
