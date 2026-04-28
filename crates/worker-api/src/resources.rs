// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU64;

use smallvec::SmallVec;
use tokio::sync::mpsc;

use restate_futures_util::concurrency::Permit;
use restate_limiter::{LimitKey, RulePattern};
use restate_memory::MemoryLease;
use restate_types::Scope;
use restate_util_string::ReString;

// This lives here temporarily until it finds a proper home
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct UserLimits {
    // None means unlimited
    pub action_concurrency: Option<NonZeroU64>,
}

impl UserLimits {
    pub fn new(action_concurrency: Option<NonZeroU64>) -> Self {
        Self { action_concurrency }
    }
}

/// Describes a rule mutation.
#[allow(dead_code)]
pub enum RuleUpdate {
    /// Insert a new rule or update an existing one with the same pattern.
    Upsert {
        pattern: RulePattern<ReString>,
        limit: UserLimits,
    },
    /// Remove a rule by its pattern.
    Remove { pattern: RulePattern<ReString> },
}

pub enum ResourceManagerUpdate {
    PermitReleased(SmallVec<[UserPermitKind; 1]>),
    RulesUpdated(RuleUpdate),
}

pub enum UserPermitKind {
    // todo: DeploymentConcurrency,
    LimitKeyConcurrency(Scope, LimitKey<ReString>),
}

#[derive(Default, Clone, Copy)]
pub struct ThrottlingToken;

/// Resources reserved from global limiters for a single invocation.
///
/// Bundles a concurrency [`Permit`] and a [`MemoryLease`] so they travel
/// together through the scheduler → leader handoff.
#[non_exhaustive]
pub struct SystemPermit {
    pub invoker_permit: Permit,
    pub throttling_permit: Option<ThrottlingToken>,
    pub memory_lease: MemoryLease,
}

impl Default for SystemPermit {
    fn default() -> Self {
        Self {
            invoker_permit: Permit::new_empty(),
            throttling_permit: None,
            memory_lease: MemoryLease::unlinked(),
        }
    }
}

impl SystemPermit {
    pub fn take(&mut self) -> SystemPermit {
        SystemPermit {
            invoker_permit: self.invoker_permit.split(1).unwrap_or(Permit::new_empty()),
            throttling_permit: self.throttling_permit.take(),
            memory_lease: self.memory_lease.take(),
        }
    }
}

// A compound permit holds a set of resources and provides remote termination access
// and signaling.
#[must_use]
#[clippy::has_significant_drop]
pub struct ReservedResources {
    resources: SmallVec<[UserPermitKind; 1]>,
    system_permit: SystemPermit,
    manager_tx: Option<mpsc::UnboundedSender<ResourceManagerUpdate>>,
}

impl ReservedResources {
    pub const fn new(
        resources: SmallVec<[UserPermitKind; 1]>,
        system_permit: SystemPermit,
        manager_tx: mpsc::UnboundedSender<ResourceManagerUpdate>,
    ) -> Self {
        Self {
            resources,
            system_permit,
            manager_tx: Some(manager_tx),
        }
    }

    // A temporary shortcut until plumbing for a new permit type is implemented
    pub fn take_invoker_permit(&mut self) -> (Permit, MemoryLease) {
        (
            self.system_permit.invoker_permit.split(1).unwrap(),
            self.system_permit.memory_lease.take(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.resources.is_empty()
    }
}

// Release the resources via a channel with the resource manager
impl Drop for ReservedResources {
    fn drop(&mut self) {
        if let Some(manager_tx) = self.manager_tx.take()
            && !self.is_empty()
        {
            let _ = manager_tx.send(ResourceManagerUpdate::PermitReleased(
                self.resources.drain(..).collect(),
            ));
        }
    }
}
