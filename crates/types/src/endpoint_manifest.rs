// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
    #![allow(clippy::derivable_impls)]

    include!(concat!(env!("OUT_DIR"), "/endpoint_manifest.rs"));
}

pub use generated::*;

use std::time::Duration;

impl From<ServiceType> for crate::invocation::ServiceType {
    fn from(value: ServiceType) -> Self {
        match value {
            ServiceType::VirtualObject => crate::invocation::ServiceType::VirtualObject,
            ServiceType::Service => crate::invocation::ServiceType::Service,
            ServiceType::Workflow => crate::invocation::ServiceType::Workflow,
        }
    }
}

impl Service {
    pub fn inactivity_timeout_duration(&self) -> Option<Duration> {
        self.inactivity_timeout
            .map(|d| Duration::from_millis(d.get()))
    }
    pub fn abort_timeout_duration(&self) -> Option<Duration> {
        self.abort_timeout.map(|d| Duration::from_millis(d.get()))
    }
    pub fn journal_retention_duration(&self) -> Option<Duration> {
        self.journal_retention
            .map(|d| Duration::from_millis(d.get()))
    }
    pub fn idempotency_retention_duration(&self) -> Option<Duration> {
        self.idempotency_retention
            .map(|d| Duration::from_millis(d.get()))
    }
}

impl Handler {
    pub fn inactivity_timeout_duration(&self) -> Option<Duration> {
        self.inactivity_timeout
            .map(|d| Duration::from_millis(d.get()))
    }
    pub fn abort_timeout_duration(&self) -> Option<Duration> {
        self.abort_timeout.map(|d| Duration::from_millis(d.get()))
    }
    pub fn journal_retention_duration(&self) -> Option<Duration> {
        self.journal_retention
            .map(|d| Duration::from_millis(d.get()))
    }
    pub fn idempotency_retention_duration(&self) -> Option<Duration> {
        self.idempotency_retention
            .map(|d| Duration::from_millis(d.get()))
    }
    pub fn workflow_completion_retention_duration(&self) -> Option<Duration> {
        self.workflow_completion_retention
            .map(|d| Duration::from_millis(d.get()))
    }
}
