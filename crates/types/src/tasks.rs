// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[derive(
    Clone,
    Debug,
    Copy,
    Hash,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub struct TaskId(u64);

// Describes the types of tasks TaskCenter manages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum_macros::IntoStaticStr, strum_macros::Display)]
pub enum TaskKind {
    RpcServer,
    RoleRunner,
    WatchDog,
}
