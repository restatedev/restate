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

use restate_worker_api::resources::{
    ReservedResources, SystemPermit, ThrottlingToken, UserPermitKind,
};

use restate_futures_util::concurrency::Permit;
use restate_types::{LockName, Scope};

use super::ResourceManager;

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

    pub(crate) fn build(self, resource_manager: &ResourceManager) -> ReservedResources {
        ReservedResources::new(
            self.user_permit.expect("user permit must be set").resources,
            self.system_permit,
            resource_manager.tx.clone(),
        )
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

// Use this to stage the resources needed to create a user permit as a transaction
#[derive(Default)]
pub(crate) struct ProvisionalPermit {
    lock: Option<CanonicalLock>,
    resources: SmallVec<[UserPermitKind; 1]>,
}

impl ProvisionalPermit {
    #[allow(dead_code)]
    pub(crate) fn add_permit(&mut self, permit: UserPermitKind) {
        self.resources.push(permit);
    }

    pub(crate) fn set_lock(&mut self, scope: Option<Scope>, lock_name: LockName) {
        self.lock = Some(CanonicalLock { scope, lock_name });
    }

    /// Atomically acquires all staged resources. This is the only place where state
    /// mutations happen — the check phase leading up to this must be side-effect free.
    pub(crate) fn secure(self, resource_manager: &mut ResourceManager) -> UserPermit {
        if let Some(lock) = self.lock.as_ref() {
            resource_manager
                .locks
                .acquire_lock(lock.scope.clone(), lock.lock_name.clone());
        }

        for resource in &self.resources {
            match resource {
                UserPermitKind::LimitKeyConcurrency(scope, limit_key) => {
                    resource_manager.user_limiter.increment_all(
                        scope,
                        limit_key,
                        super::user_limiter::LimitKind::Concurrency,
                    );
                }
            }
        }

        UserPermit {
            lock: self.lock,
            resources: self.resources,
        }
    }
}
