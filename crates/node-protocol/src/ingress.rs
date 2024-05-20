// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::ingress::{InvocationResponse, SubmittedInvocationNotification};
use serde::{Deserialize, Serialize};

use crate::common::TargetName;
use crate::define_message;

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    derive_more::From,
    strum_macros::EnumIs,
    strum_macros::IntoStaticStr,
)]
pub enum IngressMessage {
    InvocationResponse(InvocationResponse),
    SubmittedInvocationNotification(SubmittedInvocationNotification),
}

define_message! {
    @message = IngressMessage,
    @target = TargetName::Ingress,
}
