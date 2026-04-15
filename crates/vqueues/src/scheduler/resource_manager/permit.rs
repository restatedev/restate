// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use smallvec::SmallVec;
use tokio::sync::mpsc;

use restate_futures_util::concurrency::Permit;
use restate_limiter::LimitKey;
use restate_memory::MemoryLease;
use restate_types::{LockName, Scope};
use restate_util_string::ReString;

use super::{ResourceManager, ResourceManagerUpdate};

#[allow(dead_code)]
pub enum UserPermitKind {
    // todo: DeploymentConcurrency,
    LimitKeyConcurrency(Scope, LimitKey<ReString>),
}

// Holds incrementally secured resources
#[derive(Default)]
pub struct PermitBuilder {
    user_permit: Option<UserPermit>,
    system_permit: SystemPermit,
}

impl PermitBuilder {
    pub fn has_user_permit(&self) -> bool {
        self.user_permit.is_some()
    }

    pub(super) fn into_user_permit(self) -> Option<UserPermit> {
        self.user_permit
    }

    pub fn has_invoker_permit(&self) -> bool {
        !self.system_permit.invoker_permit.is_empty()
    }

    pub fn has_invoker_throttling_token(&self) -> bool {
        self.system_permit.throttling_permit.is_some()
    }

    pub fn set_invoker_permit(&mut self, invoker_permit: Permit) {
        self.system_permit.invoker_permit = invoker_permit;
    }

    pub fn set_throttling_permit(&mut self, throttling_permit: ThrottlingToken) {
        self.system_permit.throttling_permit = Some(throttling_permit);
    }

    pub(crate) fn take(&mut self) -> PermitBuilder {
        PermitBuilder {
            user_permit: self.user_permit.take(),
            system_permit: self.system_permit.take(),
        }
    }

    pub(crate) fn build(&mut self, resource_manager: &ResourceManager) -> ReservedResources {
        ReservedResources {
            resources: self
                .user_permit
                .take()
                .expect("user permit must be set")
                .resources,
            system_permit: self.system_permit.take(),
            manager_tx: Some(resource_manager.tx.clone()),
        }
    }

    pub(crate) fn set_user_permit(&mut self, user_permit: UserPermit) {
        self.user_permit = Some(user_permit);
    }
}

// Helper type, used only in this module
pub(super) struct CanonicalLock {
    pub scope: Option<Scope>,
    pub lock_name: LockName,
}

#[derive(Default)]
pub(crate) struct UserPermit {
    pub(super) lock: Option<CanonicalLock>,
    pub(super) resources: SmallVec<[UserPermitKind; 1]>,
}

#[derive(Default, Clone, Copy)]
pub struct ThrottlingToken;

/// Resources reserved from global limiters for a single invocation.
///
/// Bundles a concurrency [`Permit`] and a [`MemoryLease`] so they travel
/// together through the scheduler → leader handoff.
pub(crate) struct SystemPermit {
    invoker_permit: Permit,
    throttling_permit: Option<ThrottlingToken>,
    memory_lease: MemoryLease,
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

// Use this to stage the resources needed to create a user permit as a transaction
#[derive(Default)]
pub(crate) struct ProvisionalPermit {
    lock: Option<CanonicalLock>,
    resources: SmallVec<[UserPermitKind; 1]>,
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

impl ProvisionalPermit {
    #[allow(dead_code)]
    pub(crate) fn add_permit(&mut self, permit: UserPermitKind) {
        self.resources.push(permit);
    }

    pub(crate) fn set_lock(&mut self, scope: Option<Scope>, lock_name: LockName) {
        self.lock = Some(CanonicalLock { scope, lock_name });
    }

    pub(crate) fn secure(self, resource_manager: &mut ResourceManager) -> UserPermit {
        if let Some(lock) = self.lock.as_ref() {
            resource_manager
                .locks
                .acquire_lock(lock.scope.clone(), lock.lock_name.clone());
        }

        UserPermit {
            lock: self.lock,
            resources: self.resources,
        }
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
