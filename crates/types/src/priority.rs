// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Priority of an invocation.
///
/// Invocations with a priority other than [`Priority::Normal`] are placed in their own vqueues,
/// which the scheduler serves before vqueues of lower priority.
///
/// Since v1.8.2
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::FromRepr,
    bilrost::Enumeration,
)]
#[repr(u8)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum Priority {
    #[default]
    Normal = 0,
    Low = 1,
    High = 2,
    Live = 3,
}

impl Priority {
    /// All priorities, from the most to the least urgent.
    pub const BY_URGENCY: [Priority; 4] = [
        Priority::Live,
        Priority::High,
        Priority::Normal,
        Priority::Low,
    ];

    pub fn is_normal(&self) -> bool {
        matches!(self, Priority::Normal)
    }
}
