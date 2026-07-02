// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Well-known header names for agent audit context propagation.
//!
//! These headers carry the human-origin identity of a call chain across agent hops.
//! Restate does not auto-propagate them — the calling service/SDK is responsible
//! for re-attaching them on every outbound call, the same discipline as W3C `traceparent`.
//!
//! All other fields in a full audit trace (`agent_id`, `workflow_id`, `workflow_step`,
//! `agent_type`, `agent_version`) are already derivable from Restate's existing
//! invocation context and do not require explicit headers.
//!
//! # Enabling
//!
//! These headers are stripped at ingress by default. Set `ingress.agent-audit: true`
//! in the Restate configuration to allow them through to handlers.

/// Header carrying the human principal that originally triggered this call chain.
///
/// Value: opaque string, e.g. `"user@gov.sg"`
/// Propagation: caller must re-attach on every outbound call.
pub const TRIGGERED_BY: &str = "x-restate-audit-triggered-by";

/// Header carrying the human session/conversation that originated this call chain.
///
/// Value: opaque string, e.g. `"sess_001"`
/// Propagation: caller must re-attach on every outbound call.
pub const CONVERSATION_ID: &str = "x-restate-audit-conversation-id";

/// Prefix shared by all audit headers. Used to strip them at ingress when
/// `ingress.agent-audit` is disabled.
pub const HEADER_PREFIX: &str = "x-restate-audit-";
