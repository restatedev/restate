// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

use hashbrown::HashMap;
use tokio::task::JoinHandle;
use tracing::trace;

use restate_storage_api::StorageError;
use restate_storage_api::lock_table::LoadLocks;
use restate_types::{LockName, Scope};

use super::Waiters;
use crate::scheduler::VQueueHandle;

#[derive(Default)]
struct Inner(HashMap<LockName, Waiters>);

#[derive(Default)]
pub struct Locks {
    scoped: HashMap<Scope, Inner>,
    unscoped: Inner,
}

impl Locks {
    pub async fn create<S: LoadLocks + Send + Sync + 'static>(
        storage: S,
    ) -> Result<Self, StorageError> {
        let handle: JoinHandle<Result<_, StorageError>> = tokio::task::spawn_blocking(move || {
            let mut scoped: HashMap<Scope, Inner> = HashMap::default();
            let mut unscoped = Inner::default();
            // find and load all active locks.
            storage.scan_all_locked(|scope, lock_name| {
                if let Some(scope) = scope {
                    // Safety:
                    // In initialisation, we are confident that each lock is unique
                    // within its scope.
                    unsafe {
                        scoped
                            .entry(scope)
                            .or_default()
                            .0
                            .insert_unique_unchecked(lock_name, Default::default());
                    }
                } else {
                    unsafe {
                        unscoped
                            .0
                            .insert_unique_unchecked(lock_name, Default::default());
                    }
                }
            })?;
            Ok((scoped, unscoped))
        });

        let (scoped, unscoped) = handle
            .await
            .map_err(|e| StorageError::Generic(e.into()))??;
        Ok(Self { scoped, unscoped })
    }

    pub(super) fn release_lock(
        &mut self,
        scope: &Option<Scope>,
        lock_name: &LockName,
    ) -> Option<VecDeque<VQueueHandle>> {
        if let Some(scope) = scope {
            trace!("Releasing lock {lock_name} in scope {scope}");
            let scoped_locks = self.scoped.get_mut(scope).unwrap();
            scoped_locks.0.remove(lock_name)
        } else {
            trace!("Releasing unscoped lock {lock_name}");
            self.unscoped.0.remove(lock_name)
        }
    }

    /// Panics if the lock doesn't exist.
    pub(super) fn add_to_waiters(
        &mut self,
        handle: VQueueHandle,
        scope: &Option<Scope>,
        lock_name: &LockName,
    ) {
        if let Some(scope) = scope {
            let scoped_locks = self.scoped.get_mut(scope).unwrap();
            scoped_locks.0.get_mut(lock_name).unwrap().push_back(handle);
        } else {
            self.unscoped
                .0
                .get_mut(lock_name)
                .unwrap()
                .push_back(handle);
        }
    }

    pub(super) fn remove_from_waiters(
        &mut self,
        handle: VQueueHandle,
        scope: &Option<Scope>,
        lock_name: &LockName,
    ) {
        if let Some(scope) = scope {
            let Some(scoped_locks) = self.scoped.get_mut(scope) else {
                return;
            };
            let Some(waiters) = scoped_locks.0.get_mut(lock_name) else {
                return;
            };
            waiters.retain(|h| *h != handle);
        } else {
            let Some(waiters) = self.unscoped.0.get_mut(lock_name) else {
                return;
            };
            waiters.retain(|h| *h != handle);
        }
    }

    pub fn is_locked(&mut self, scope: &Option<Scope>, lock_name: &LockName) -> bool {
        if let Some(scope) = scope {
            let scoped_locks = self.scoped.get_mut(scope).unwrap();
            scoped_locks.0.contains_key(lock_name)
        } else {
            self.unscoped.0.contains_key(lock_name)
        }
    }

    pub fn acquire_lock(&mut self, scope: Option<Scope>, lock_name: LockName) {
        if let Some(scope) = scope {
            trace!("Acquiring lock {lock_name} in scope {scope}");
            let scoped_locks = self.scoped.entry(scope).or_default();
            let _existing = scoped_locks.0.insert(lock_name, Default::default());
            debug_assert!(_existing.is_none());
        } else {
            trace!("Acquired unscoped lock {lock_name}");
            let _existing = self.unscoped.0.insert(lock_name, Default::default());
            debug_assert!(_existing.is_none());
        }
    }
}
