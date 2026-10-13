// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub(crate) mod busy_vqueue;
pub(crate) mod by_service;
pub(crate) mod by_virtual_object;
pub(crate) mod entry_by_stage;
pub(crate) mod entry_next_at_by_service;
pub(crate) mod entry_next_at_by_stage;
pub(crate) mod entry_next_at_by_virtual_object;
mod table;

#[cfg(test)]
mod tests;
