// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Command pool: loads pre-generated commands and serves them to the benchmark loop.

use restate_types::logs::Lsn;
use restate_wal_protocol::Command;

/// Pre-loaded pool of commands that the benchmark loop draws from.
/// Cycles through the pool when the benchmark needs more commands than are stored.
pub struct CommandPool {
    commands: Vec<Command>,
    lsns: Option<Vec<Lsn>>,
    index: usize,
}

impl CommandPool {
    /// Create a pool from commands without LSNs (generated workloads).
    pub fn new(commands: Vec<Command>) -> Self {
        assert!(
            !commands.is_empty(),
            "CommandPool must have at least one command"
        );
        Self {
            commands,
            lsns: None,
            index: 0,
        }
    }

    /// Create a pool from commands with associated LSNs (extracted workloads).
    pub fn with_lsns(commands: Vec<Command>, lsns: Vec<Lsn>) -> Self {
        assert!(
            !commands.is_empty(),
            "CommandPool must have at least one command"
        );
        assert_eq!(commands.len(), lsns.len(), "commands and LSNs must match");
        Self {
            commands,
            lsns: Some(lsns),
            index: 0,
        }
    }

    /// Take the next command (and optional LSN) from the pool, cycling when exhausted.
    pub fn next_command(&mut self) -> (Command, Option<Lsn>) {
        let cmd = self.commands[self.index].clone();
        let lsn = self.lsns.as_ref().map(|l| l[self.index]);
        self.index = (self.index + 1) % self.commands.len();
        (cmd, lsn)
    }

    /// Whether this pool carries real LSNs from an extracted workload.
    pub fn has_lsns(&self) -> bool {
        self.lsns.is_some()
    }

    /// Number of unique commands in the pool.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}
