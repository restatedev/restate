// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tools for shutting down shared resources safely, while other code may still
//! be using them.
//!
//! Before a shared resource, such as a database, can be shut down, two
//! questions need an answer:
//!
//! 1. **Has all the work that uses it finished?** Some of that work may run on
//!    other threads, or wait in a queue. A [`Gate`] counts it, and can refuse
//!    new work.
//! 2. **Does anyone still hold it?** Even after all work has finished, other
//!    code may still hold a reference to the resource. A [`KeepAlive`] keeps
//!    the resource alive until its last holder lets go of it.
//!
//! A [`Scope`] combines the two for resources that live inside other
//! resources, like tables inside a database. Scopes form a tree: each one
//! keeps its parent alive, and shutting down a parent also shuts down
//! everything inside it. The documentation of [`Scope`] shows all the pieces
//! working together.

mod gate;
mod keep_alive;
mod scope;

pub use gate::{Gate, Lease, OwnedLease};
pub use keep_alive::{KeepAlive, KeepAliveTarget};
pub use scope::{Node, Scope, ScopeParent, ScopePayload};
