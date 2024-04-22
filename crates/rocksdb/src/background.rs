// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Instant;

use derive_builder::Builder;
use metrics::histogram;

use crate::metric_definitions::{
    STORAGE_BG_TASK_RUN_DURATION, STORAGE_BG_TASK_TOTAL_DURATION, STORAGE_BG_TASK_WAIT_DURATION,
};
use crate::{DbName, Owner, Priority};

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum_macros::IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum StorageTaskKind {
    WriteBatch,
    OpenColumnFamily,
    FlushWal,
    Shutdown,
    OpenDb,
}

#[derive(Builder)]
#[builder(pattern = "owned")]
#[builder(name = "StorageTask")]
pub struct ReadyStorageTask<OP> {
    op: OP,
    #[builder(setter(into))]
    db_name: DbName,
    #[builder(default)]
    owner: Owner,
    #[builder(default)]
    pub(crate) priority: Priority,
    /// required
    kind: StorageTaskKind,
    #[builder(setter(skip))]
    #[builder(default = "Instant::now()")]
    enqueue_at: Instant,
}

impl<OP, R> ReadyStorageTask<OP>
where
    OP: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    pub fn run(self) -> R {
        let start = Instant::now();
        let kind: &'static str = self.kind.into();
        let owner: &'static str = self.owner.into();
        let priority: &'static str = self.priority.into();
        histogram!(
            STORAGE_BG_TASK_WAIT_DURATION,
            "kind" => kind,
            "db" => self.db_name.to_string(),
            "owner" => owner,
            "priority" => priority,
        )
        .record(self.enqueue_at.elapsed());
        let res = (self.op)();
        histogram!(STORAGE_BG_TASK_RUN_DURATION,
            "kind" => kind,
            "db" => self.db_name.to_string(),
            "owner" => owner,
            "priority" => priority,
        )
        .record(start.elapsed());
        histogram!(STORAGE_BG_TASK_TOTAL_DURATION,
            "kind" => kind,
            "db" => self.db_name.to_string(),
            "owner" => owner,
            "priority" => priority,
        )
        .record(self.enqueue_at.elapsed());
        res
    }
}
