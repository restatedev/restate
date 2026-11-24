// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Validates an action message and turns it into what the runner does with it: an action to
//! propose with the results it owes, an answer at once, or a read.

use std::num::NonZeroU32;

use bytes::Bytes;
use bytestring::ByteString;

use restate_service_protocol_explicit_recovery::codec::Message;
use restate_service_protocol_explicit_recovery::proto::{
    self, StateKeys, Value, Void, atomic_group_action_message::action::Action as GroupMember,
    attach_invocation_action_message::Target as AttachTarget,
    result_notification_message::Result as NotificationResult,
};
use restate_types::Scope;
use restate_types::errors::{InvocationError, codes};
use restate_types::identifiers::{IdempotencyId, InvocationId, ServiceId};
use restate_types::invocation::{
    Header, InvocationQuery, InvocationTarget, InvocationTargetType, ResponseResult,
    ServiceInvocationSpanContext, SpanRelation,
};
use restate_types::journal_v2::{CallRequest, Failure, FailureMetadata, SignalResult};
use restate_types::limit_key::LimitKey;
use restate_types::schema::invocation_target::{DeploymentStatus, InvocationTargetResolver};
use restate_types::time::MillisSinceEpoch;
use restate_util_string::ReString;
use restate_worker_api::invoker::explicit_recovery::{ErAction, ErStateScope};

use super::book::{AttemptBook, Violation};

/// What the action is about: the invocation that runs it.
pub(crate) struct ActionContext<'a> {
    pub invocation_id: InvocationId,
    pub invocation_target: &'a InvocationTarget,
    pub span_context: &'a ServiceInvocationSpanContext,
}

/// A read: served once every action sent before it is applied (G6, G8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Read {
    GetState {
        scope: ErStateScope,
        key: Bytes,
        result_id: u32,
    },
    GetStateKeys {
        scope: ErStateScope,
        result_id: u32,
    },
    Signal {
        name: ByteString,
        index: u32,
        result_id: u32,
    },
    /// `Flush`, and the spans: a void result, if wanted, once everything before is applied.
    Barrier {
        result_id: u32,
    },
}

#[derive(Debug)]
pub(crate) enum Classified {
    /// Proposed as one effect. `internal` are sent once it is applied; `external` arrive later.
    Propose {
        action: ErAction,
        highest_result_id: Option<NonZeroU32>,
        internal: Vec<(u32, NotificationResult)>,
        external: Vec<u32>,
    },
    /// Answered at once, without proposing anything.
    Answer(Vec<(u32, NotificationResult)>),
    Read(Read),
}

pub(crate) fn void() -> NotificationResult {
    NotificationResult::Void(Void {})
}

pub(crate) fn value(content: Bytes) -> NotificationResult {
    NotificationResult::Value(Value { content })
}

pub(crate) fn from_response_result(result: ResponseResult) -> NotificationResult {
    match result {
        ResponseResult::Success(content) => value(content),
        ResponseResult::Failure(err) => NotificationResult::Failure(proto::Failure {
            code: u32::from(u16::from(err.code())),
            message: err.message().to_owned(),
            metadata: err
                .metadata()
                .iter()
                .map(|(key, value)| proto::FailureMetadata {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
        }),
    }
}

pub(crate) fn from_signal_result(result: SignalResult) -> NotificationResult {
    match result {
        SignalResult::Void => void(),
        SignalResult::Success(content) => value(content),
        SignalResult::Failure(failure) => NotificationResult::Failure(proto::Failure {
            code: u32::from(u16::from(failure.code)),
            message: failure.message.to_string(),
            metadata: failure
                .metadata
                .into_iter()
                .map(|m| proto::FailureMetadata {
                    key: m.key.to_string(),
                    value: m.value.to_string(),
                })
                .collect(),
        }),
    }
}

pub(crate) fn to_invocation_error(failure: proto::Failure) -> InvocationError {
    InvocationError::new(clamp_code(failure.code), failure.message).with_metadata_vec(
        failure
            .metadata
            .into_iter()
            .map(|m| (m.key, m.value))
            .collect(),
    )
}

fn clamp_code(code: u32) -> u16 {
    u16::try_from(code).unwrap_or(500)
}

fn to_signal_failure(failure: proto::Failure) -> Failure {
    Failure {
        code: clamp_code(failure.code).into(),
        message: failure.message.into(),
        metadata: failure
            .metadata
            .into_iter()
            .map(|m| FailureMetadata {
                key: m.key.into(),
                value: m.value.into(),
            })
            .collect(),
    }
}

fn content(value: Option<Value>) -> Bytes {
    value.map(|v| v.content).unwrap_or_default()
}

fn scope_of(scope: i32) -> Result<ErStateScope, Violation> {
    match proto::StateScope::try_from(scope) {
        Ok(proto::StateScope::Object) => Ok(ErStateScope::Object),
        Ok(proto::StateScope::Invocation) => Ok(ErStateScope::Invocation),
        _ => Err(Violation::new(format!(
            "state scope {scope} is not a scope"
        ))),
    }
}

fn parse_invocation_id(id: &str) -> Result<InvocationId, Violation> {
    id.parse()
        .map_err(|e| Violation::new(format!("malformed invocation id '{id}': {e}")))
}

fn required(result_id: u32, what: &str) -> Result<u32, Violation> {
    if result_id == 0 {
        return Err(Violation::new(format!("{what} without its result id")));
    }
    Ok(result_id)
}

impl ActionContext<'_> {
    fn check_object_scope(&self, scope: ErStateScope, write: bool) -> Result<(), Violation> {
        if scope != ErStateScope::Object {
            return Ok(());
        }
        let ty = self.invocation_target.invocation_target_ty();
        if self.invocation_target.key().is_none() {
            return Err(Violation::new(
                "object state used by a handler of a target without a key",
            ));
        }
        if write && !ty.can_write_state() {
            return Err(Violation::new("object state written by a shared handler"));
        }
        Ok(())
    }

    /// Classifies an action message. Checks its result ids in wire order, and the spans.
    pub(crate) fn classify(
        &self,
        message: Message,
        book: &mut AttemptBook,
        resolver: &impl InvocationTargetResolver,
    ) -> Result<Classified, Violation> {
        Ok(match message {
            Message::GetStateAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, false)?;
                let result_id = required(m.result_id, "GetState")?;
                book.use_id(result_id)?;
                Classified::Read(Read::GetState {
                    scope,
                    key: m.key,
                    result_id,
                })
            }
            Message::GetStateKeysAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, false)?;
                let result_id = required(m.result_id, "GetStateKeys")?;
                book.use_id(result_id)?;
                Classified::Read(Read::GetStateKeys { scope, result_id })
            }
            Message::ReadSignalAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let result_id = required(m.result_id, "ReadSignal")?;
                book.use_id(result_id)?;
                Classified::Read(Read::Signal {
                    name: m.signal_name.into(),
                    index: m.signal_index,
                    result_id,
                })
            }
            Message::FlushAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let result_id = required(m.result_id, "Flush")?;
                book.use_id(result_id)?;
                Classified::Read(Read::Barrier { result_id })
            }
            Message::SpanStartAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                book.start_span(m.span_id)?;
                book.use_id(m.result_id)?;
                Classified::Read(Read::Barrier {
                    result_id: m.result_id,
                })
            }
            Message::SpanEndAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                book.end_span(m.span_id)?;
                book.use_id(m.result_id)?;
                Classified::Read(Read::Barrier {
                    result_id: m.result_id,
                })
            }
            Message::AttachInvocationAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let result_id = required(m.result_id, "AttachInvocation")?;
                book.use_id(result_id)?;
                let query = attach_query(m.target)?;
                if query.to_invocation_id() == self.invocation_id {
                    // D55: an attach to itself fails at once.
                    return Ok(Classified::Answer(vec![(
                        result_id,
                        NotificationResult::Failure(proto::Failure {
                            code: u32::from(u16::from(codes::BAD_REQUEST)),
                            message: "an invocation cannot attach to itself".to_owned(),
                            metadata: vec![],
                        }),
                    )]));
                }
                Classified::Propose {
                    action: ErAction::Attach {
                        query,
                        result_id: NonZeroU32::new(result_id).expect("checked"),
                    },
                    highest_result_id: NonZeroU32::new(result_id),
                    internal: vec![],
                    external: vec![result_id],
                }
            }
            Message::AtomicGroupAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                if m.actions.is_empty() {
                    return Err(Violation::new("an atomic group with no member"));
                }
                let mut members = Vec::with_capacity(m.actions.len());
                let mut internal = vec![];
                let mut external = vec![];
                for member in m.actions {
                    let Some(member) = member.action else {
                        return Err(Violation::new("a member of a group with no action set"));
                    };
                    let message = match member {
                        GroupMember::SetState(m) => Message::SetStateAction(m),
                        GroupMember::ClearState(m) => Message::ClearStateAction(m),
                        GroupMember::ClearAllState(m) => Message::ClearAllStateAction(m),
                        GroupMember::Call(m) => Message::CallAction(m),
                        GroupMember::CancelInvocation(m) => Message::CancelInvocationAction(m),
                        GroupMember::SendSignal(m) => Message::SendSignalAction(m),
                        GroupMember::SpanStart(m) => Message::SpanStartAction(m),
                        GroupMember::SpanEnd(m) => Message::SpanEndAction(m),
                    };
                    match self.classify(message, book, resolver)? {
                        Classified::Propose {
                            action,
                            internal: member_internal,
                            external: member_external,
                            ..
                        } => {
                            members.push(action);
                            internal.extend(member_internal);
                            external.extend(member_external);
                        }
                        // A span in a group: its void result comes with the group's.
                        Classified::Read(Read::Barrier { result_id }) => {
                            if result_id != 0 {
                                internal.push((result_id, void()));
                            }
                        }
                        Classified::Answer(_) | Classified::Read(_) => {
                            unreachable!("only the members a group may hold are classified")
                        }
                    }
                }
                book.use_id(m.result_id)?;
                if m.result_id != 0 {
                    internal.push((m.result_id, void()));
                }
                Classified::Propose {
                    action: ErAction::Group(members),
                    highest_result_id: NonZeroU32::new(book.last_result_id()),
                    internal,
                    external,
                }
            }
            Message::SetStateAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, true)?;
                book.use_id(m.result_id)?;
                propose_void(
                    ErAction::SetState {
                        scope,
                        key: m.key,
                        value: content(m.value),
                    },
                    m.result_id,
                )
            }
            Message::ClearStateAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, true)?;
                book.use_id(m.result_id)?;
                propose_void(ErAction::ClearState { scope, key: m.key }, m.result_id)
            }
            Message::ClearAllStateAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let scope = scope_of(m.scope)?;
                self.check_object_scope(scope, true)?;
                book.use_id(m.result_id)?;
                propose_void(ErAction::ClearAllState { scope }, m.result_id)
            }
            Message::CancelInvocationAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let target = parse_invocation_id(&m.invocation_id)?;
                book.use_id(m.result_id)?;
                propose_void(ErAction::CancelInvocation { target }, m.result_id)
            }
            Message::SendSignalAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let target = parse_invocation_id(&m.target_invocation_id)?;
                book.use_id(m.result_id)?;
                let result = match m.result {
                    None | Some(proto::send_signal_action_message::Result::Void(_)) => {
                        SignalResult::Void
                    }
                    Some(proto::send_signal_action_message::Result::Value(v)) => {
                        SignalResult::Success(v.content)
                    }
                    Some(proto::send_signal_action_message::Result::Failure(f)) => {
                        SignalResult::Failure(to_signal_failure(f))
                    }
                };
                propose_void(
                    ErAction::SendSignal {
                        target,
                        name: m.signal_name.into(),
                        result,
                    },
                    m.result_id,
                )
            }
            Message::CallAction(m) => {
                book.check_parent_span(m.parent_span_id)?;
                let id_result_id = required(m.invocation_id_result_id, "Call")?;
                book.use_id_pair(id_result_id, m.result_id)?;
                let result_id = NonZeroU32::new(m.result_id);
                let execution_time =
                    (m.invoke_time != 0).then(|| MillisSinceEpoch::new(m.invoke_time));
                let (request, uninvokable) = self.resolve_call(resolver, m);
                let mut internal = vec![(
                    id_result_id,
                    NotificationResult::InvocationId(request.invocation_id.to_string()),
                )];
                let mut external = vec![];
                if let Some(result_id) = result_id {
                    match &uninvokable {
                        // D22: the target cannot be invoked: its result is a failure, right
                        // after the id.
                        Some(err) => internal.push((
                            result_id.get(),
                            from_response_result(ResponseResult::Failure(err.clone())),
                        )),
                        None => external.push(result_id.get()),
                    }
                }
                Classified::Propose {
                    action: ErAction::Call {
                        request: Box::new(request),
                        execution_time,
                        result_id,
                        uninvokable,
                    },
                    highest_result_id: NonZeroU32::new(id_result_id.max(m_result(result_id))),
                    internal,
                    external,
                }
            }
            other => {
                return Err(Violation::new(format!(
                    "unexpected message {:?}",
                    other.ty()
                )));
            }
        })
    }

    /// Resolves the target of a call, as the durable execution protocol does. A target that
    /// cannot be invoked still gets an invocation id (D22, step 1).
    fn resolve_call(
        &self,
        resolver: &impl InvocationTargetResolver,
        m: proto::CallActionMessage,
    ) -> (CallRequest, Option<InvocationError>) {
        let headers: Vec<Header> = m
            .headers
            .into_iter()
            .map(|h| Header::new(h.key, h.value))
            .collect();
        let idempotency_key: Option<ByteString> = m.idempotency_key.map(Into::into);
        let service_name: ByteString = m.service_name.into();
        let handler_name: ByteString = m.handler_name.into();
        let key: ByteString = m.key.into();

        let resolved = (|| {
            let meta = resolver
                .resolve_latest_invocation_target(&service_name, &handler_name)
                .ok_or_else(|| {
                    InvocationError::new(
                        codes::NOT_FOUND,
                        format!("the service handler {service_name}/{handler_name} was not found"),
                    )
                })?;
            if let DeploymentStatus::Deprecated(dp_id) = meta.deployment_status {
                return Err(InvocationError::new(
                    codes::NOT_FOUND,
                    format!(
                        "the service {service_name} is exposed by the deprecated deployment {dp_id}"
                    ),
                ));
            }
            if !key.is_empty() && !meta.target_ty.is_keyed() {
                return Err(InvocationError::new(
                    codes::BAD_REQUEST,
                    format!("the service {service_name} has no key, but the call has one"),
                ));
            }
            let scope = match m.scope.as_deref() {
                Some(scope) if !scope.is_empty() => {
                    Some(Scope::try_non_interned(scope).map_err(|e| {
                        InvocationError::new(
                            codes::BAD_REQUEST,
                            format!("invalid scope '{scope}': {e}"),
                        )
                    })?)
                }
                _ => None,
            };
            let target = match meta.target_ty {
                InvocationTargetType::Service => {
                    InvocationTarget::service(service_name.clone(), handler_name.clone())
                }
                InvocationTargetType::VirtualObject(h_ty) => InvocationTarget::virtual_object(
                    service_name.clone(),
                    key.clone(),
                    handler_name.clone(),
                    h_ty,
                ),
                InvocationTargetType::Workflow(h_ty) => InvocationTarget::workflow(
                    service_name.clone(),
                    key.clone(),
                    handler_name.clone(),
                    h_ty,
                ),
            }
            .with_scope(scope);
            if idempotency_key.as_ref().is_some_and(|k| k.is_empty()) {
                return Err(InvocationError::new(
                    codes::BAD_REQUEST,
                    "the idempotency key is empty",
                ));
            }
            let limit_key: LimitKey<ReString> = match &m.limit_key {
                Some(limit_key) => limit_key.parse().map_err(|e| {
                    InvocationError::new(
                        codes::BAD_REQUEST,
                        format!("invalid limit key '{limit_key}': {e}"),
                    )
                })?,
                None => LimitKey::None,
            };
            if !limit_key.is_empty() && target.scope().is_none() {
                return Err(InvocationError::new(
                    codes::BAD_REQUEST,
                    "a limit key needs a scope",
                ));
            }
            Ok((target, meta, limit_key))
        })();

        match resolved {
            Ok((target, meta, limit_key)) => {
                let retention = meta.compute_retention(idempotency_key.is_some());
                let invocation_id = InvocationId::generate(&target, idempotency_key.as_deref());
                let span_context = ServiceInvocationSpanContext::start(
                    &invocation_id,
                    SpanRelation::parent(self.span_context.span_context().clone()),
                );
                (
                    CallRequest {
                        invocation_id,
                        invocation_target: target,
                        span_context,
                        parameter: content(m.parameter),
                        headers,
                        idempotency_key,
                        completion_retention_duration: retention.completion_retention,
                        journal_retention_duration: retention.journal_retention,
                        limit_key,
                        protocol_family: meta.protocol_family,
                    },
                    None,
                )
            }
            Err(err) => {
                // Mint the id from the names, so that a key or an idempotency key still gives a
                // deterministic partition.
                let target = if key.is_empty() {
                    InvocationTarget::service(service_name, handler_name)
                } else {
                    InvocationTarget::virtual_object(
                        service_name,
                        key,
                        handler_name,
                        restate_types::invocation::VirtualObjectHandlerType::Exclusive,
                    )
                };
                let invocation_id = InvocationId::generate(
                    &target,
                    idempotency_key.as_deref().filter(|k| !k.is_empty()),
                );
                let request = CallRequest {
                    invocation_id,
                    invocation_target: target,
                    span_context: ServiceInvocationSpanContext::empty(),
                    parameter: Bytes::new(),
                    headers,
                    idempotency_key,
                    completion_retention_duration: Default::default(),
                    journal_retention_duration: Default::default(),
                    limit_key: LimitKey::None,
                    protocol_family: Default::default(),
                };
                (request, Some(err))
            }
        }
    }
}

fn m_result(result_id: Option<NonZeroU32>) -> u32 {
    result_id.map(NonZeroU32::get).unwrap_or(0)
}

fn propose_void(action: ErAction, result_id: u32) -> Classified {
    Classified::Propose {
        action,
        highest_result_id: NonZeroU32::new(result_id),
        internal: if result_id != 0 {
            vec![(result_id, void())]
        } else {
            vec![]
        },
        external: vec![],
    }
}

fn attach_query(target: Option<AttachTarget>) -> Result<InvocationQuery, Violation> {
    let parse_scope = |scope: Option<String>| -> Result<Option<Scope>, Violation> {
        match scope {
            Some(scope) if !scope.is_empty() => Scope::try_non_interned(&scope)
                .map(Some)
                .map_err(|e| Violation::new(format!("invalid scope '{scope}': {e}"))),
            _ => Ok(None),
        }
    };
    Ok(match target {
        None => return Err(Violation::new("an AttachInvocation with no target")),
        Some(AttachTarget::InvocationId(id)) => {
            InvocationQuery::Invocation(parse_invocation_id(&id)?)
        }
        Some(AttachTarget::IdempotentRequestTarget(t)) => {
            InvocationQuery::IdempotencyId(IdempotencyId::new(
                t.service_name.into(),
                t.service_key.map(Into::into),
                t.handler_name.into(),
                t.idempotency_key.into(),
                parse_scope(t.scope)?,
            ))
        }
        Some(AttachTarget::WorkflowTarget(t)) => InvocationQuery::Workflow(ServiceId::new(
            parse_scope(t.scope)?,
            t.workflow_name,
            t.workflow_key,
        )),
    })
}

/// The keys of a scope, as a result.
pub(crate) fn state_keys(keys: Vec<Bytes>) -> NotificationResult {
    NotificationResult::StateKeys(StateKeys { keys })
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_types::invocation::ServiceType;
    use restate_types::invocation::VirtualObjectHandlerType;
    use restate_types::schema::invocation_target::InvocationTargetMetadata;
    use restate_types::schema::invocation_target::test_util::MockInvocationTargetResolver;

    fn set_state(scope: proto::StateScope, result_id: u32) -> proto::SetStateActionMessage {
        proto::SetStateActionMessage {
            scope: scope.into(),
            key: Bytes::from_static(b"k"),
            value: Some(Value {
                content: Bytes::from_static(b"v"),
            }),
            result_id,
            ..Default::default()
        }
    }

    #[test]
    fn groups_calls_and_violations() {
        let target = InvocationTarget::mock_virtual_object();
        let span_context = ServiceInvocationSpanContext::empty();
        let ctx = ActionContext {
            invocation_id: InvocationId::mock_generate(&target),
            invocation_target: &target,
            span_context: &span_context,
        };
        let mut resolver = MockInvocationTargetResolver::default();
        resolver.add(
            "callee",
            ServiceType::VirtualObject,
            [(
                "h",
                InvocationTargetMetadata::mock(InvocationTargetType::VirtualObject(
                    VirtualObjectHandlerType::Exclusive,
                )),
            )],
        );
        let mut book = AttemptBook::new(5, false, 2);

        // A group: a write of each scope and a call; ids in wire order, the group's last.
        let call = proto::CallActionMessage {
            service_name: "callee".into(),
            handler_name: "h".into(),
            key: "k".into(),
            invocation_id_result_id: 8,
            result_id: 7,
            ..Default::default()
        };
        let group = proto::AtomicGroupActionMessage {
            actions: [
                GroupMember::SetState(set_state(proto::StateScope::Object, 5)),
                GroupMember::SetState(set_state(proto::StateScope::Invocation, 0)),
                GroupMember::Call(call.clone()),
            ]
            .into_iter()
            .map(|a| proto::atomic_group_action_message::Action { action: Some(a) })
            .collect(),
            result_id: 9,
            ..Default::default()
        };
        let Classified::Propose {
            action: ErAction::Group(members),
            highest_result_id,
            internal,
            external,
        } = ctx
            .classify(Message::AtomicGroupAction(group), &mut book, &resolver)
            .unwrap()
        else {
            panic!("a group is proposed");
        };
        assert_eq!(members.len(), 3);
        assert_eq!(highest_result_id, NonZeroU32::new(9));
        assert_eq!(
            internal.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![5, 8, 9]
        );
        assert_eq!(external, vec![7]);

        // A call to a target that cannot be invoked: its id, then a failure, nothing external.
        let unknown = proto::CallActionMessage {
            service_name: "unknown".into(),
            invocation_id_result_id: 10,
            result_id: 11,
            ..call
        };
        let Classified::Propose {
            action: ErAction::Call { uninvokable, .. },
            internal,
            external,
            ..
        } = ctx
            .classify(Message::CallAction(unknown), &mut book, &resolver)
            .unwrap()
        else {
            panic!("a call is proposed");
        };
        assert!(uninvokable.is_some());
        assert!(matches!(
            internal.as_slice(),
            [
                (10, NotificationResult::InvocationId(_)),
                (11, NotificationResult::Failure(_))
            ]
        ));
        assert!(external.is_empty());

        // Violations: an id out of order, a required id missing, an empty group, scope 0.
        for message in [
            Message::SetStateAction(set_state(proto::StateScope::Object, 11)),
            Message::FlushAction(proto::FlushActionMessage::default()),
            Message::AtomicGroupAction(proto::AtomicGroupActionMessage::default()),
            Message::SetStateAction(set_state(proto::StateScope::Unspecified, 0)),
        ] {
            assert!(ctx.classify(message, &mut book, &resolver).is_err());
        }

        // Attach to itself: answered at once with a failure.
        assert!(matches!(
            ctx.classify(
                Message::AttachInvocationAction(proto::AttachInvocationActionMessage {
                    target: Some(AttachTarget::InvocationId(ctx.invocation_id.to_string())),
                    result_id: 20,
                    ..Default::default()
                }),
                &mut book,
                &resolver
            )
            .unwrap(),
            Classified::Answer(_)
        ));
    }
}
