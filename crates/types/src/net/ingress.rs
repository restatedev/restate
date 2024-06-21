// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::ingress::{InvocationResponse, SubmittedInvocationNotification};
use crate::net::{define_message, TargetName};

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
