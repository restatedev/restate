// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[derive(Debug, Clone, Default, Copy, PartialEq, Eq, Hash, bilrost::Message)]
pub struct InboxEntry {
    /// How many times did this entry get attempts to run.
    /// This is 0 if the entry was never attempted to run (new)
    ///
    /// We increment this on every attempt to run. For instance,
    /// If the entry was suspended and resumed it would have its num_attempts > 0.
    #[bilrost(1)]
    pub(crate) num_attempts: u32,
    /// Tells whether entry is currently holding a concurrency token or not.
    #[bilrost(2)]
    pub(crate) token_held: bool,
}

impl InboxEntry {
    #[inline]
    pub fn was_never_attempted(&self) -> bool {
        self.num_attempts == 0
    }

    #[inline]
    pub fn increment_run_attempts(&mut self) {
        self.num_attempts += 1;
    }

    #[inline]
    pub fn is_token_held(&self) -> bool {
        self.token_held
    }

    #[inline]
    pub fn release_held_token(&mut self) {
        self.token_held = false;
    }

    #[inline]
    pub fn hold_token(&mut self) {
        self.token_held = true;
    }
}
