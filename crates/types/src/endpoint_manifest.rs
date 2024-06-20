// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod generated {
    #![allow(clippy::clone_on_copy)]
    #![allow(clippy::to_string_trait_impl)]

    include!(concat!(env!("OUT_DIR"), "/endpoint_manifest.rs"));
}

pub use generated::*;

impl From<ServiceType> for crate::invocation::ServiceType {
    fn from(value: ServiceType) -> Self {
        match value {
            ServiceType::VirtualObject => crate::invocation::ServiceType::VirtualObject,
            ServiceType::Service => crate::invocation::ServiceType::Service,
            ServiceType::Workflow => crate::invocation::ServiceType::Workflow,
        }
    }
}
