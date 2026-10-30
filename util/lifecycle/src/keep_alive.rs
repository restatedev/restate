// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

/// Keeps a shared resource alive, and through it everything that resource
/// depends on.
///
/// A `KeepAlive` is a shared pointer, like an [`Arc`]: the resource is dropped
/// only after the last `KeepAlive` that points to it is dropped. If the
/// resource itself holds a `KeepAlive` on another resource, that one stays
/// alive too.
///
/// It comes in two forms:
///
/// | Form           | What it can do                                                     |
/// |----------------|--------------------------------------------------------------------|
/// | `KeepAlive<T>` | Knows the resource's type, and gives shared access to it.          |
/// | `KeepAlive`    | Only keeps the resource alive, and can tell its name.              |
///
/// `KeepAlive<T>` derefs to the `Arc<T>` it holds, so it can be passed wherever
/// a `&Arc<T>` is expected.
///
/// The second form works for any kind of resource, so code that deals with many
/// kinds at once, such as a queue of background tasks, can store it.
/// [`KeepAlive::erase`] turns the first form into the second.
///
/// ```
/// use std::sync::Arc;
///
/// use restate_util_lifecycle::{KeepAlive, KeepAliveTarget};
///
/// struct Cache {
///     entries: Vec<u64>,
/// }
///
/// impl KeepAliveTarget for Cache {
///     fn name(&self) -> &str {
///         "cache"
///     }
/// }
///
/// let cache = KeepAlive::from(Arc::new(Cache { entries: vec![1, 2, 3] }));
///
/// // The typed form gives access to the resource.
/// assert_eq!(cache.entries.len(), 3);
///
/// // A queued task only needs the cache to stay alive until the task is done.
/// // The queue holds tasks for all kinds of resources, so it stores the plain
/// // form.
/// let task_keeps_alive: KeepAlive = cache.clone().erase();
/// assert_eq!(task_keeps_alive.name(), "cache");
/// ```
///
/// Clones are cheap: one atomic increment, and no allocation.
pub struct KeepAlive<T: ?Sized = dyn KeepAliveTarget>(Arc<T>);

impl<T: KeepAliveTarget> KeepAlive<T> {
    /// Forgets the resource's type. The result still keeps the resource alive,
    /// and can still tell its name.
    pub fn erase(self) -> KeepAlive {
        KeepAlive(self.0)
    }
}

impl<T: ?Sized + KeepAliveTarget> From<Arc<T>> for KeepAlive<T> {
    /// Starts keeping `target` alive. It won't be dropped before this value and
    /// all of its clones are dropped.
    fn from(target: Arc<T>) -> Self {
        Self(target)
    }
}

impl<T: ?Sized> Clone for KeepAlive<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: ?Sized> Deref for KeepAlive<T> {
    type Target = Arc<T>;

    fn deref(&self) -> &Arc<T> {
        &self.0
    }
}

impl<T: ?Sized + KeepAliveTarget> fmt::Debug for KeepAlive<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("KeepAlive")
            .field(&KeepAliveTarget::name(&*self.0))
            .finish()
    }
}

/// A resource that a [`KeepAlive`] can point to.
pub trait KeepAliveTarget: Send + Sync + 'static {
    /// A name that tells which resource this is, for example in a log line
    /// about what a shutdown is still waiting for.
    fn name(&self) -> &str;
}

static_assertions::assert_impl_all!(KeepAlive: Send, Sync, Clone);
// The plain form needs a pointer to the resource and one to its vtable. The
// typed form needs only the first.
static_assertions::assert_eq_size!(KeepAlive, [usize; 2]);
static_assertions::assert_eq_size!(KeepAlive<String>, usize);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{KeepAlive, KeepAliveTarget};

    struct Resource {
        id: u32,
    }

    impl KeepAliveTarget for Resource {
        fn name(&self) -> &str {
            "resource"
        }
    }

    #[test]
    fn keeps_its_target_alive_until_the_last_clone_is_dropped() {
        let resource = Arc::new(Resource { id: 7 });
        let weak = Arc::downgrade(&resource);
        let typed = KeepAlive::from(resource);
        assert_eq!(Arc::strong_count(&typed), 1);
        assert_eq!(typed.id, 7);
        let erased = typed.clone().erase();
        assert_eq!(erased.name(), "resource");
        assert_eq!(format!("{typed:?}"), r#"KeepAlive("resource")"#);
        assert_eq!(format!("{erased:?}"), r#"KeepAlive("resource")"#);

        drop(typed);
        assert!(weak.upgrade().is_some());
        drop(erased);
        assert!(weak.upgrade().is_none());
    }
}
