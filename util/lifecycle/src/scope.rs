// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use restate_util_string::ReString;

use crate::{Gate, KeepAlive, KeepAliveTarget};

/// A shared resource that lives inside another resource, together with a gate
/// for the work that uses it.
///
/// Scopes form a tree, like tables inside a database. Each scope has:
///
/// - a **payload**: the resource itself (see [`ScopePayload`]). The payload
///   holds a [`KeepAlive`] on the parent, so the parent stays alive as long as
///   the scope does.
/// - a **gate** for the work that uses the resource (see [`Gate`]). It is a
///   child of the parent's gate, so shutting down the parent also shuts down
///   the scope.
/// - a **name**, for reports about what a shutdown is still waiting for.
///
/// A scope is a shared pointer, like a [`KeepAlive`]: clones are cheap and
/// point to the same scope. Its methods are listed under [`Node`].
///
/// # Example
///
/// A database holds a table. Each time the table is opened, a session controls
/// the work of that opening, so that closing the table can wait for that work
/// to finish. Each level keeps the one above it alive:
///
/// ```text
/// database   KeepAlive<Database>
///  └─ orders    Scope<Table>
///      └─ session   Scope<Scope<Table>>
/// ```
///
/// ```
/// use std::sync::Arc;
///
/// use restate_util_lifecycle::{
///     Gate, KeepAlive, KeepAliveTarget, Scope, ScopeParent, ScopePayload,
/// };
///
/// // The top of the tree. It isn't inside any scope, so it has its own gate.
/// struct Database {
///     gate: Arc<Gate>,
/// }
///
/// impl KeepAliveTarget for Database {
///     fn name(&self) -> &str {
///         "database"
///     }
/// }
///
/// impl ScopeParent for Database {
///     fn gate(&self) -> &Arc<Gate> {
///         &self.gate
///     }
/// }
///
/// // A table, which lives inside the database.
/// struct Table {
///     rows: Vec<String>,
///     // Declared last, so that it is dropped last.
///     database: KeepAlive<Database>,
/// }
///
/// impl ScopePayload for Table {
///     type Parent = Database;
///
///     fn parent(&self) -> &KeepAlive<Database> {
///         &self.database
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let database = KeepAlive::from(Arc::new(Database { gate: Arc::default() }));
/// let rows = vec!["first order".to_owned()];
/// let table = Scope::new("orders", Table { rows, database });
///
/// // A session has no resources of its own, so the table's scope is its
/// // whole payload.
/// let session = Scope::new("orders session", table.clone());
///
/// // Work takes a lease from the session before it uses the table.
/// let lease = session.gate().try_acquire().expect("the session is open");
/// let table_in_session: &Table = session.payload().payload();
/// assert_eq!(table_in_session.rows.len(), 1);
/// drop(lease);
///
/// // Closing the table for this session: refuse new work, then wait for the
/// // work that is still running.
/// session.gate().seal();
/// session.gate().drain().await;
///
/// // Deleting the table: close it, which also closes all of its sessions.
/// // Then wait until nobody holds the table anymore.
/// table.gate().seal();
/// table.gate().drain().await;
/// let released = table.released();
/// drop(session);
/// drop(table);
/// released.await;
/// // Now it is safe to delete the table's files.
/// # }
/// ```
///
/// # Closing and releasing
///
/// The example ends a scope in two steps. Each step answers a different
/// question:
///
/// | Step    | How                                                  | Afterwards, you know that                                                   |
/// |---------|------------------------------------------------------|-----------------------------------------------------------------------------|
/// | Close   | [`Gate::seal`], then wait for [`Gate::drain`]        | No work uses the scope or the scopes inside it, and no new work can start. |
/// | Release | Take [`Node::released`], drop your clones, then wait | Nobody holds the scope anymore, and its payload has been dropped.           |
///
/// Closing doesn't release anything: a closed table still holds its files, and
/// could be opened again with a new session. Releasing is what makes it safe
/// to get rid of what the scope stood for.
pub type Scope<T> = KeepAlive<Node<T>>;

/// The resource that a [`Scope`] holds.
///
/// A payload keeps its parent alive by owning a [`KeepAlive`] on it, and
/// returns it from [`ScopePayload::parent`]. The scope reads the parent's gate
/// from there when it is created.
///
/// # Declare the keep-alive last
///
/// Rust drops the fields of a struct in the order they are declared. If
/// another field depends on the parent, for example a handle that is only valid
/// while the parent exists, declare the keep-alive as the *last* field. Then
/// the handle is dropped first, while the parent is still alive:
///
/// ```
/// # use restate_util_lifecycle::KeepAlive;
/// # struct Database;
/// # struct FileHandle;
/// struct Table {
///     file: FileHandle,              // Dropped first.
///     database: KeepAlive<Database>, // Dropped last.
/// }
/// ```
///
/// # A scope without resources of its own
///
/// A `KeepAlive<P>` is a payload too, and its parent is `P`. Use it for a scope
/// that needs its own gate but holds nothing else, such as a session (see the
/// example on [`Scope`]).
pub trait ScopePayload: Send + Sync + 'static {
    /// The resource that this payload lives inside.
    type Parent: ?Sized + ScopeParent;

    /// The keep-alive that this payload holds on its parent.
    fn parent(&self) -> &KeepAlive<Self::Parent>;
}

/// A resource that scopes can be created inside.
///
/// Every scope can be the parent of other scopes. Implement this trait for a
/// resource at the top of a tree, which isn't inside any scope itself. The
/// example on [`Scope`] implements it for a database.
pub trait ScopeParent: KeepAliveTarget {
    /// The gate that the gates of new scopes inside this resource become
    /// children of.
    fn gate(&self) -> &Arc<Gate>;
}

impl<P: ?Sized + ScopeParent> ScopePayload for KeepAlive<P> {
    type Parent = P;

    fn parent(&self) -> &KeepAlive<P> {
        self
    }
}

/// What a [`Scope`] points to. A scope gives access to these methods directly.
pub struct Node<T> {
    payload: T,
    gate: Arc<Gate>,
    name: ReString,
    // Last, so it fires only after the payload has been dropped.
    released: Release,
}

impl<T: ScopePayload> Scope<T> {
    /// Creates a scope for `payload`, inside the parent that the payload holds.
    ///
    /// The scope's gate becomes a child of the parent's gate. If the parent's
    /// gate is already sealed, the new scope starts out sealed too.
    pub fn new(name: impl Into<ReString>, payload: T) -> Self {
        let gate = payload.parent().gate().child();
        KeepAlive::from(Arc::new(Node {
            payload,
            gate,
            name: name.into(),
            released: Release(CancellationToken::new()),
        }))
    }
}

impl<T: ScopePayload> Node<T> {
    /// The resource that this scope holds.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// The gate for the work that uses this scope.
    pub fn gate(&self) -> &Arc<Gate> {
        &self.gate
    }

    /// The name that the scope was created with.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a future that completes once the scope has been dropped,
    /// together with everything in its payload.
    ///
    /// Call this while you still hold the scope, then drop your clones and
    /// wait. The future doesn't keep the scope alive.
    ///
    /// If the scope is dropped while a panic unwinds, the future never
    /// completes. Whoever waits to clean up after the scope then keeps waiting,
    /// instead of cleaning up after a scope that may have been left in a broken
    /// state.
    pub fn released(&self) -> impl Future<Output = ()> + Send + 'static + use<T> {
        self.released.0.clone().cancelled_owned()
    }
}

impl<T: ScopePayload> KeepAliveTarget for Node<T> {
    fn name(&self) -> &str {
        &self.name
    }
}

impl<T: ScopePayload> ScopeParent for Node<T> {
    fn gate(&self) -> &Arc<Gate> {
        &self.gate
    }
}

struct Release(CancellationToken);

impl Drop for Release {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.0.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::{Context, Waker};

    use super::*;

    struct Database {
        gate: Arc<Gate>,
    }

    impl KeepAliveTarget for Database {
        fn name(&self) -> &str {
            "database"
        }
    }

    impl ScopeParent for Database {
        fn gate(&self) -> &Arc<Gate> {
            &self.gate
        }
    }

    struct Table {
        database: KeepAlive<Database>,
    }

    impl ScopePayload for Table {
        type Parent = Database;

        fn parent(&self) -> &KeepAlive<Database> {
            &self.database
        }
    }

    fn is_pending(future: impl Future) -> bool {
        pin!(future)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    }

    #[test]
    fn scopes_keep_their_parents_alive_and_shut_down_with_them() {
        let database = Arc::new(Database {
            gate: Arc::default(),
        });
        let weak_database = Arc::downgrade(&database);
        let table = Scope::new(
            "table",
            Table {
                database: KeepAlive::from(database.clone()),
            },
        );
        let session = Scope::new("session", table.clone());
        assert_eq!(session.name(), "session");
        assert_eq!(session.payload().name(), "table");
        assert_eq!(session.payload().payload().parent().name(), "database");
        assert_eq!(
            format!("{:?}", session.clone().erase()),
            r#"KeepAlive("session")"#
        );

        // Sealing the database seals the scopes below it.
        let lease = session.gate().try_acquire().unwrap();
        database.gate.seal();
        assert!(table.gate().try_acquire().is_none());
        assert!(session.gate().try_acquire().is_none());
        drop(lease);

        // The session keeps the table alive, and the table keeps the database
        // alive. The release fires only after the payload is gone.
        let mut released = pin!(table.released());
        drop(table);
        drop(database);
        assert!(is_pending(released.as_mut()));
        drop(session);
        assert!(!is_pending(released.as_mut()));
        assert!(weak_database.upgrade().is_none());
    }
}
