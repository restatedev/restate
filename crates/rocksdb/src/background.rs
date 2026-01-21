// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use metrics::{gauge, histogram};
use tokio::sync::oneshot;

use crate::metric_definitions::{STORAGE_BG_TASK_RUN_DURATION, STORAGE_BG_TASK_WAIT_DURATION};
use crate::{OP_TYPE, PRIORITY, Priority, STORAGE_BG_TASK_IN_FLIGHT};

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "kebab-case")]
pub enum StorageTaskKind {
    WriteBatch,
    OpenColumnFamily,
    ImportColumnFamily,
    ExportColumnFamily,
    FlushWal,
    FlushMemtables,
    Compaction,
    Shutdown,
    OpenDb,
    BackgroundIterator,
}

impl StorageTaskKind {
    pub fn as_static_str(&self) -> &'static str {
        self.into()
    }
}

#[derive(Builder)]
#[builder(pattern = "owned")]
#[builder(name = "StorageTask")]
pub struct ReadyStorageTask<OP> {
    op: OP,
    #[builder(default)]
    pub(crate) priority: Priority,
    /// required
    kind: StorageTaskKind,
    #[builder(setter(skip))]
    #[builder(default = "Instant::now()")]
    created_at: Instant,
}

impl<OP, R> ReadyStorageTask<OP>
where
    OP: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    pub fn into_runner(self) -> impl FnOnce() -> R + Send + 'static {
        gauge!(STORAGE_BG_TASK_IN_FLIGHT,
         PRIORITY => self.priority.as_static_str(),
         OP_TYPE => self.kind.as_static_str(),
        )
        .increment(1);

        let span = tracing::Span::current().clone();

        move || span.in_scope(|| self.run())
    }

    pub fn into_async_runner(self, tx: oneshot::Sender<R>) -> impl FnOnce() + Send + 'static {
        gauge!(STORAGE_BG_TASK_IN_FLIGHT,
         PRIORITY => self.priority.as_static_str(),
         OP_TYPE => self.kind.as_static_str(),
        )
        .increment(1);
        let span = tracing::Span::current().clone();

        move || {
            span.in_scope(|| {
                let result = self.run();
                let _ = tx.send(result);
            })
        }
    }

    fn run(self) -> R {
        let start = Instant::now();
        let labels = [(OP_TYPE, self.kind.as_static_str())];
        histogram!(STORAGE_BG_TASK_WAIT_DURATION, &labels).record(self.created_at.elapsed());
        let res = (self.op)();
        histogram!(STORAGE_BG_TASK_RUN_DURATION, &labels).record(start.elapsed());
        gauge!(STORAGE_BG_TASK_IN_FLIGHT,
             PRIORITY => self.priority.as_static_str(),
             OP_TYPE => self.kind.as_static_str(),
        )
        .decrement(1);
        res
    }
}
