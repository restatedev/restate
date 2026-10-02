// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::invocation::InvocationTarget;
use crate::state_mut::ExternalStateMutation;

/// Borrows a common queue-entry target view from an invocation target or an
/// external state mutation, without allocating or cloning the underlying strings.
pub trait EntryTargetExt {
    /// Returns the entry's service, scope, key, and handler information.
    fn entry_target_ref(&self) -> EntryTargetRef<'_>;
}

/// Distinguishes a named virtual-object handler from an external state mutation.
///
/// State mutations target an object but do not invoke a user handler.
#[derive(Debug)]
pub enum HandlerRef<'a> {
    /// The borrowed name of the invocation's handler.
    UserHandler(&'a str),
    /// An external state mutation with no user-handler name.
    StateMutation,
}

/// Borrowed target information shared by invocation and state-mutation queue entries.
///
/// Lifecycle callers supply this view alongside the entry's before/after state so
/// consumers can access target information without owning an invocation or mutation.
/// External state mutations are represented as [`Self::VirtualObject`] with
/// [`HandlerRef::StateMutation`]. All string references borrow from the source target.
#[derive(Debug)]
pub enum EntryTargetRef<'a> {
    /// An invocation of an unkeyed service handler.
    Service {
        scope: Option<&'a str>,
        service: &'a str,
        handler: &'a str,
    },
    /// A virtual-object invocation or an external mutation of that object's state.
    VirtualObject {
        scope: Option<&'a str>,
        service: &'a str,
        key: &'a str,
        handler: HandlerRef<'a>,
    },
    /// An invocation of a workflow handler, identified by its workflow key.
    Workflow {
        scope: Option<&'a str>,
        service: &'a str,
        key: &'a str,
        handler: &'a str,
    },
}

impl EntryTargetRef<'_> {
    /// Returns the service name for any kind of target.
    pub fn service(&self) -> &str {
        match self {
            Self::Service { service, .. } => service,
            Self::VirtualObject { service, .. } => service,
            Self::Workflow { service, .. } => service,
        }
    }

    /// Returns the object key for virtual-object invocations and state mutations.
    /// Service and workflow targets return `None`.
    pub fn virtual_object_key(&self) -> Option<&str> {
        match self {
            Self::Service { .. } | Self::Workflow { .. } => None,
            Self::VirtualObject { key, .. } => Some(key),
        }
    }

    /// Returns the workflow key, or `None` for service and virtual-object targets.
    pub fn workflow_key(&self) -> Option<&str> {
        match self {
            Self::Service { .. } | Self::VirtualObject { .. } => None,
            Self::Workflow { key, .. } => Some(key),
        }
    }

    /// Returns the target's optional scope.
    pub fn scope(&self) -> Option<&str> {
        match self {
            Self::Service { scope, .. } => scope.as_deref(),
            Self::VirtualObject { scope, .. } => scope.as_deref(),
            Self::Workflow { scope, .. } => scope.as_deref(),
        }
    }

    /// Returns the invocation's handler name, or `None` for an external state mutation.
    pub fn handler(&self) -> Option<&str> {
        match self {
            Self::Service { handler, .. } => Some(handler),
            Self::VirtualObject {
                handler: HandlerRef::UserHandler(handler),
                ..
            } => Some(handler),
            Self::VirtualObject {
                handler: HandlerRef::StateMutation,
                ..
            } => None,
            Self::Workflow { handler, .. } => Some(handler),
        }
    }
}

impl EntryTargetExt for InvocationTarget {
    fn entry_target_ref(&self) -> EntryTargetRef<'_> {
        match self {
            InvocationTarget::Service {
                name,
                handler,
                scope,
            } => EntryTargetRef::Service {
                scope: scope.as_ref().map(AsRef::as_ref),
                service: name,
                handler,
            },
            InvocationTarget::VirtualObject {
                scope,
                name,
                handler,
                key,
                ..
            } => EntryTargetRef::VirtualObject {
                scope: scope.as_ref().map(AsRef::as_ref),
                service: name,
                key,
                handler: HandlerRef::UserHandler(handler.as_ref()),
            },
            InvocationTarget::Workflow {
                name,
                handler,
                scope,
                key,
                ..
            } => EntryTargetRef::Workflow {
                scope: scope.as_ref().map(AsRef::as_ref),
                service: name,
                key,
                handler,
            },
        }
    }
}

impl EntryTargetExt for ExternalStateMutation {
    fn entry_target_ref(&self) -> EntryTargetRef<'_> {
        EntryTargetRef::VirtualObject {
            scope: self.service_id.scope.as_ref().map(AsRef::as_ref),
            service: &self.service_id.service_name,
            key: &self.service_id.key,
            handler: HandlerRef::StateMutation,
        }
    }
}
