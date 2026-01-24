// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut};

const LOG_ENTRY_KEY_LENGTH: usize = 9;
const LOG_ENTRY_KEY_PREFIX: u8 = b'l';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LogEntryKey {
    index: u64,
}

impl LogEntryKey {
    pub fn new(index: u64) -> Self {
        LogEntryKey { index }
    }

    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn encode<B: BufMut + ?Sized>(self, buf: &mut B) {
        buf.put_u8(LOG_ENTRY_KEY_PREFIX);
        buf.put_u64(self.index);
    }

    pub fn to_bytes(self) -> [u8; LOG_ENTRY_KEY_LENGTH] {
        let mut buffer = [0; LOG_ENTRY_KEY_LENGTH];
        let mut slice = &mut buffer[..];
        self.encode(&mut slice);
        buffer
    }

    /// panics if the prefix is not a log entry
    pub fn from_slice(mut data: &[u8]) -> Self {
        let log_entry_key_prefix = data.get_u8();
        debug_assert_eq!(log_entry_key_prefix, LOG_ENTRY_KEY_PREFIX);
        let index = data.get_u64();
        Self { index }
    }
}

pub(super) const CONF_STATE_KEY: &[u8] = b"conf-state";
pub(super) const HARD_STATE_KEY: &[u8] = b"hard-state";
pub(super) const SNAPSHOT_KEY: &[u8] = b"snapshot";
pub(super) const NODES_CONFIGURATION_KEY: &[u8] = b"nodes-configuration";
pub(super) const RAFT_SERVER_STATE_KEY: &[u8] = b"raft-server-state";
// storage marker
pub(super) const MARKER_KEY: &[u8] = b"storage-marker";
