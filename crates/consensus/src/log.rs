// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::sender::{StateMachineOwnedPermit, StateMachineSender};
use crate::Command;
use std::collections::VecDeque;
use std::mem;

#[derive(Debug)]
pub(super) struct CommandLog<Cmd> {
    state: State<Cmd>,
    log: VecDeque<Command<Cmd>>,
}

#[derive(Debug)]
enum State<Cmd> {
    Empty(StateMachineSender<Command<Cmd>>),
    WaitingForSendCapacity,
}

impl<Cmd> CommandLog<Cmd> {
    pub(super) fn new(state_machine_tx: StateMachineSender<Command<Cmd>>) -> Self {
        Self {
            state: State::Empty(state_machine_tx),
            log: Default::default(),
        }
    }

    /// Appends the given command to the log. Returns the corresponding sender if it requires
    /// waiting for send capacity.
    pub(super) fn append_cmd(
        &mut self,
        cmd: Command<Cmd>,
    ) -> Option<StateMachineSender<Command<Cmd>>> {
        self.log.push_back(cmd);

        let state = mem::replace(&mut self.state, State::WaitingForSendCapacity);

        if let State::Empty(sender) = state {
            Some(sender)
        } else {
            None
        }
    }

    /// Applies the next command by sending it to the given channel. Returns the sender if it
    /// requires waiting for send capacity afterwards.
    pub(super) fn apply_next_cmd(
        &mut self,
        permit: StateMachineOwnedPermit<Command<Cmd>>,
    ) -> Option<StateMachineSender<Command<Cmd>>> {
        debug_assert!(
            matches!(self.state, State::WaitingForSendCapacity),
            "Expect command log to wait for send capacity."
        );

        if let Some(cmd) = self.log.pop_front() {
            let sender = permit.send(cmd);

            if self.log.is_empty() {
                self.state = State::Empty(sender);
                None
            } else {
                Some(sender)
            }
        } else {
            let sender = permit.release();
            self.state = State::Empty(sender);
            None
        }
    }
}
