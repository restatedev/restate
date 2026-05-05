// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Admin endpoints for the cluster-global limiter rule book.
//!
//! Mutations go straight to the metadata store via
//! [`MetadataStoreClient::read_modify_write`], which handles the
//! fetch-modify-CAS retry loop for us.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use restate_admin_rest_model::rules::{CreateRuleRequest, PatchRuleRequest, RuleResponse};
use restate_limiter::{NewRule, RuleBook, RuleBookError, RuleChange, RuleId, RulePatch};
use restate_metadata_store::ReadModifyWriteError;
use restate_types::metadata_store::keys::RULE_BOOK_KEY;

use crate::rest_api::ErrorDescriptionResponse;
use crate::state::AdminServiceState;

// -- Errors ---------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub(crate) enum RulesApiError {
    #[error("rule '{0}' already exists")]
    AlreadyExists(RuleId),
    #[error("rule '{0}' not found")]
    NotFound(RuleId),
    #[error("rule book is full ({cap} rules)")]
    CapExceeded { cap: usize },
    #[error("metadata store I/O failed: {0}")]
    MetadataStore(#[from] restate_metadata_store::ReadWriteError),
    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for RulesApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            RulesApiError::AlreadyExists(_) => StatusCode::CONFLICT,
            RulesApiError::NotFound(_) => StatusCode::NOT_FOUND,
            RulesApiError::CapExceeded { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            RulesApiError::MetadataStore(_) => StatusCode::INTERNAL_SERVER_ERROR,
            RulesApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(ErrorDescriptionResponse {
                message: self.to_string(),
                restate_code: None,
            }),
        )
            .into_response()
    }
}

impl utoipa::IntoResponses for RulesApiError {
    fn responses() -> std::collections::BTreeMap<
        String,
        utoipa::openapi::RefOr<utoipa::openapi::response::Response>,
    > {
        use std::collections::BTreeMap;
        use utoipa::openapi::{Ref, RefOr};

        let mut responses = BTreeMap::new();
        responses.insert(
            "404".to_string(),
            RefOr::Ref(Ref::from_response_name("NotFound")),
        );
        responses.insert(
            "409".to_string(),
            RefOr::Ref(Ref::from_response_name("Conflict")),
        );
        responses.insert(
            "422".to_string(),
            RefOr::Ref(Ref::from_response_name("UnprocessableEntity")),
        );
        responses.insert(
            "500".to_string(),
            RefOr::Ref(Ref::from_response_name("InternalServerError")),
        );
        responses
    }
}

impl From<ReadModifyWriteError<RulesApiError>> for RulesApiError {
    fn from(err: ReadModifyWriteError<RulesApiError>) -> Self {
        match err {
            ReadModifyWriteError::FailedOperation(inner) => inner,
            ReadModifyWriteError::ReadWrite(rw_err) => RulesApiError::MetadataStore(rw_err),
        }
    }
}

// -- Handlers -------------------------------------------------------------

/// Create a rule.
///
/// The server derives the rule's `rul_…` id from the canonical display
/// form of `pattern`, so two rules with the same pattern collide.
#[utoipa::path(
    post,
    path = "/limits/rules",
    operation_id = "create_rule",
    tag = "rule",
    request_body = CreateRuleRequest,
    responses(
        (status = 201, description = "Rule created", body = RuleResponse, headers(
            ("Location" = String, description = "URI of the created rule")
        )),
        RulesApiError,
    )
)]
pub async fn create_rule<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(payload): Json<CreateRuleRequest>,
) -> Result<impl IntoResponse, RulesApiError> {
    let rule_id = RuleId::from(&payload.pattern);

    let new_rule = NewRule {
        pattern: payload.pattern,
        limits: payload.limits,
        description: payload.description,
        disabled: payload.disabled,
    };

    let book = state
        .metadata_store_client
        .read_modify_write::<RuleBook, _, RulesApiError>(RULE_BOOK_KEY.clone(), |old| {
            let mut book = old.unwrap_or_default();
            book.apply_change(rule_id, RuleChange::Create(new_rule.clone()))
                .map_err(map_rule_book_error)?;
            Ok(book)
        })
        .await?;

    let rule = book
        .get(&rule_id)
        .expect("rule must exist after a successful create")
        .clone();
    let rule_response = RuleResponse::from((rule_id, rule));

    notify_rule_book_observer(&state, book);

    Ok((
        StatusCode::CREATED,
        [(header::LOCATION, format!("limits/rules/{}", rule_id))],
        Json(rule_response),
    ))
}

/// Patch a rule.
///
/// JSON Merge Patch on `limits.action_concurrency`/`description` (omit
/// to keep, `null` to clear, value to set) and `disabled` (omit to
/// keep, boolean to set). Pattern cannot be changed — delete and
/// recreate.
#[utoipa::path(
    patch,
    path = "/limits/rules/{rule_id}",
    operation_id = "update_rule",
    tag = "rule",
    params(
        ("rule_id" = String, Path, description = "Rule identifier (rul_…)"),
    ),
    request_body = PatchRuleRequest,
    responses(
        (status = 200, description = "Rule updated", body = RuleResponse),
        RulesApiError,
    )
)]
pub async fn update_rule<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(rule_id): Path<RuleId>,
    Json(payload): Json<PatchRuleRequest>,
) -> Result<Json<RuleResponse>, RulesApiError> {
    let patch = RulePatch {
        limits: payload.limits,
        description: payload.description,
        disabled: payload.disabled,
    };

    let book = state
        .metadata_store_client
        .read_modify_write::<RuleBook, _, RulesApiError>(RULE_BOOK_KEY.clone(), |old| {
            let mut book = old.ok_or(RulesApiError::NotFound(rule_id))?;
            book.apply_change(rule_id, RuleChange::Patch(patch.clone()))
                .map_err(map_rule_book_error)?;
            Ok(book)
        })
        .await?;

    let rule = book
        .get(&rule_id)
        .expect("patched rule must exist in the result")
        .clone();
    let rule_response = RuleResponse::from((rule_id, rule));

    notify_rule_book_observer(&state, book);
    Ok(Json(rule_response))
}

/// Delete a rule.
#[utoipa::path(
    delete,
    path = "/limits/rules/{rule_id}",
    operation_id = "delete_rule",
    tag = "rule",
    params(
        ("rule_id" = String, Path, description = "Rule identifier (rul_…)"),
    ),
    responses(
        (status = 204, description = "Rule deleted"),
        RulesApiError,
    )
)]
pub async fn delete_rule<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(rule_id): Path<RuleId>,
) -> Result<StatusCode, RulesApiError> {
    let book = state
        .metadata_store_client
        .read_modify_write::<RuleBook, _, RulesApiError>(RULE_BOOK_KEY.clone(), |old| {
            let mut book = old.ok_or(RulesApiError::NotFound(rule_id))?;
            book.apply_change(rule_id, RuleChange::Delete)
                .map_err(map_rule_book_error)?;
            Ok(book)
        })
        .await?;

    notify_rule_book_observer(&state, book);

    Ok(StatusCode::NO_CONTENT)
}

fn notify_rule_book_observer<Metadata, Discovery, Telemetry, Invocations, Transport>(
    state: &AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>,
    book: RuleBook,
) {
    if let Some(observer) = &state.rule_book_observer {
        observer(book);
    }
}

fn map_rule_book_error(e: RuleBookError) -> RulesApiError {
    match e {
        RuleBookError::AlreadyExists(id) => RulesApiError::AlreadyExists(id),
        RuleBookError::NotFound(id) => RulesApiError::NotFound(id),
        RuleBookError::CapExceeded { cap } => RulesApiError::CapExceeded { cap },
        RuleBookError::IdPatternMismatch => RulesApiError::Internal("The id does not match to the pattern. Please inform the Restate developers or file a Github issue.".to_owned())
    }
}
