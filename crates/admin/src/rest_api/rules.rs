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
//! fetch-modify-CAS retry loop for us. Each handler accepts a batch
//! body and forwards it to [`RuleBook::apply_changes`], so the entire
//! request commits atomically or not at all.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use restate_admin_rest_model::rules::{DeleteRuleRequest, RuleResponse, UpsertRuleRequest};
use restate_limiter::{Precondition, RuleBook, RuleBookError, RuleChange, RulePattern, RuleUpsert};
use restate_metadata_store::ReadModifyWriteError;
use restate_types::metadata_store::keys::RULE_BOOK_KEY;
use restate_util_string::ReString;

use crate::rest_api::ErrorDescriptionResponse;
use crate::state::AdminServiceState;

// -- Errors ---------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub(crate) enum RulesApiError {
    #[error(transparent)]
    RuleBook(#[from] RuleBookError),
    #[error("metadata store I/O failed: {0}")]
    MetadataStore(#[from] restate_metadata_store::ReadWriteError),
}

impl IntoResponse for RulesApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            RulesApiError::RuleBook(RuleBookError::PreconditionFailed { .. }) => {
                StatusCode::CONFLICT
            }
            RulesApiError::RuleBook(RuleBookError::CapExceeded { .. }) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            RulesApiError::MetadataStore(_) => StatusCode::INTERNAL_SERVER_ERROR,
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

/// Upsert a batch of rules.
///
/// Each entry carries an optional [`Precondition`]. Setting it to
/// `DoesNotExist` makes the entry a strict insert; `Matches(v)` rejects
/// the batch unless the rule's current version is `v`; omitting it
/// (`None`) is unconditional. The whole batch is atomic: any failed
/// precondition or cap-exceeded condition rolls back the rest.
#[utoipa::path(
    put,
    path = "/limits/rules",
    operation_id = "upsert_rules",
    tag = "rule",
    request_body = Vec<UpsertRuleRequest>,
    responses(
        (status = 200, description = "Rules upserted", body = Vec<RuleResponse>),
        RulesApiError,
    )
)]
pub async fn upsert_rules<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(payload): Json<Vec<UpsertRuleRequest>>,
) -> Result<Json<Vec<RuleResponse>>, RulesApiError> {
    let book = state
        .metadata_store_client
        .read_modify_write::<RuleBook, _, RulesApiError>(RULE_BOOK_KEY.clone(), |old| {
            let mut book = old.unwrap_or_default();
            let changes = payload.iter().map(|entry| {
                (
                    entry.pattern.clone(),
                    RuleChange::Upsert(RuleUpsert {
                        limits: entry.limits.clone(),
                        description: entry.description.clone(),
                        disabled: entry.disabled,
                        precondition: entry.precondition,
                    }),
                )
            });
            book.apply_changes(changes)?;
            Ok(book)
        })
        .await?;

    let response: Vec<RuleResponse> = payload
        .into_iter()
        .filter_map(|entry| {
            book.get(&entry.pattern)
                .map(|rule| RuleResponse::from((entry.pattern, rule)))
        })
        .collect();

    notify_rule_book_observer(&state, book);
    Ok(Json(response))
}

/// Delete a batch of rules by pattern.
///
/// Each entry may carry an `expected_version`; if present the rule
/// must exist at that exact version, otherwise the batch fails.
/// Without `expected_version` the delete is unconditional and
/// idempotent: a pattern that's already absent is silently skipped.
/// Returns the patterns that this batch actually removed.
#[utoipa::path(
    post,
    path = "/limits/rules/bulk-delete",
    operation_id = "bulk_delete_rules",
    tag = "rule",
    request_body = Vec<DeleteRuleRequest>,
    responses(
        (status = 200, description = "Patterns that were actually removed", body = Vec<String>),
        RulesApiError,
    )
)]
pub async fn delete_rules<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(payload): Json<Vec<DeleteRuleRequest>>,
) -> Result<Json<Vec<String>>, RulesApiError> {
    let mut removed: Vec<RulePattern<ReString>> = Vec::with_capacity(payload.len());

    let book = state
        .metadata_store_client
        .read_modify_write::<RuleBook, _, RulesApiError>(RULE_BOOK_KEY.clone(), |old| {
            // Re-record on every retry so the final value reflects the
            // rule book at the successful CAS.
            removed.clear();
            if let Some(book) = &old {
                for entry in &payload {
                    if book.get(&entry.pattern).is_some() {
                        removed.push(entry.pattern.clone());
                    }
                }
            }

            let mut book = old.unwrap_or_default();
            let changes = payload.iter().map(|entry| {
                let precondition = match entry.expected_version {
                    Some(v) => Precondition::Matches(v),
                    None => Precondition::None,
                };
                (entry.pattern.clone(), RuleChange::Delete { precondition })
            });
            book.apply_changes(changes)?;
            Ok(book)
        })
        .await?;

    notify_rule_book_observer(&state, book);
    Ok(Json(
        removed
            .into_iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>(),
    ))
}

fn notify_rule_book_observer<Metadata, Discovery, Telemetry, Invocations, Transport>(
    state: &AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>,
    book: RuleBook,
) {
    if let Some(observer) = &state.rule_book_observer {
        observer.notify_observed(book);
    }
}
