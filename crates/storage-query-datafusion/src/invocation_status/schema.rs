// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use crate::table_macro::*;

use datafusion::arrow::datatypes::DataType;

define_table!(sys_invocation_status(
    /// Internal column that is used for partitioning the services invocations. Can be ignored.
    partition_key: DataType::UInt64,

    /// [Invocation ID](/operate/invocation#invocation-identifier).
    id: DataType::LargeUtf8,

    /// Either `inboxed` or `scheduled` or `invoked` or `suspended` or `completed`
    status: DataType::LargeUtf8,

    /// If `status = 'completed'`, this contains either `success` or `failure`
    completion_result: DataType::LargeUtf8,

    /// If `status = 'completed' AND completion_result = 'failure'`, this contains the error cause
    completion_failure: DataType::LargeUtf8,

    /// Invocation Target. Format for plain services: `ServiceName/HandlerName`, e.g.
    /// `Greeter/greet`. Format for virtual objects/workflows: `VirtualObjectName/Key/HandlerName`,
    /// e.g. `Greeter/Francesco/greet`.
    target: DataType::LargeUtf8,

    /// The name of the invoked service.
    target_service_name: DataType::LargeUtf8,

    /// The key of the virtual object or the workflow ID. Null for regular services.
    target_service_key: DataType::LargeUtf8,

    /// The invoked handler.
    target_handler_name: DataType::LargeUtf8,

    /// The service type. Either `service` or `virtual_object` or `workflow`.
    target_service_ty: DataType::LargeUtf8,

    /// Either `ingress` if the service was invoked externally or `service` if the service was
    /// invoked by another Restate service.
    invoked_by: DataType::LargeUtf8,

    /// The name of the invoking service. Or `null` if invoked externally.
    invoked_by_service_name: DataType::LargeUtf8,

    /// The caller [Invocation ID](/operate/invocation#invocation-identifier) if the service was
    /// invoked by another Restate service. Or `null` if invoked externally.
    invoked_by_id: DataType::LargeUtf8,

    /// The caller invocation target if the service was invoked by another Restate service. Or
    /// `null` if invoked externally.
    invoked_by_target: DataType::LargeUtf8,

    /// The ID of the service deployment that started processing this invocation, and will continue
    /// to do so (e.g. for retries). This gets set after the first journal entry has been stored for
    /// this invocation.
    pinned_deployment_id: DataType::LargeUtf8,

    /// The negotiated protocol version used for this invocation.
    /// This gets set after the first journal entry has been stored for this invocation.
    pinned_service_protocol_version: DataType::UInt32,

    /// The ID of the trace that is assigned to this invocation. Only relevant when tracing is
    /// enabled.
    trace_id: DataType::LargeUtf8,

    /// The number of journal entries durably logged for this invocation.
    journal_size: DataType::UInt32,

    /// Timestamp indicating the start of this invocation.
    created_at: DataType::Date64,

    /// Timestamp indicating the last invocation status transition. For example, last time the
    /// status changed from `invoked` to `suspended`.
    modified_at: DataType::Date64,

    /// Timestamp indicating when the invocation was inboxed, if ever.
    inboxed_at: DataType::Date64,

    /// Timestamp indicating when the invocation was scheduled, if ever.
    scheduled_at: DataType::Date64,

    /// Timestamp indicating when the invocation first transitioned to running, if ever.
    running_at: DataType::Date64,

    /// Timestamp indicating when the invocation was completed, if ever.
    completed_at: DataType::Date64,
));
