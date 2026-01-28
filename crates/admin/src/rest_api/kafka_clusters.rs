// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::error::*;
use crate::state::AdminServiceState;
use std::time::SystemTime;

use restate_admin_rest_model::kafka_clusters::*;
use restate_admin_rest_model::subscriptions::SubscriptionResponse;

use axum::extract::Query;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Json, http};
use restate_errors::warn_it;
use restate_types::schema::registry::{AllowOrphanSubscriptions, MetadataService};
use serde::Deserialize;

/// Create Kafka cluster
///
/// Registers a new Kafka cluster configuration that can be referenced by subscriptions.
/// The cluster configuration is validated to ensure required broker properties are present.
#[utoipa::path(
    post,
    path = "/kafka-clusters",
    operation_id = "create_kafka_cluster",
    tag = "kafka_cluster",
    request_body = CreateKafkaClusterRequest,
    responses(
        (status = 201, description = "Kafka cluster created successfully", body = SimpleKafkaClusterResponse, headers(
            ("Location" = String, description = "URI of the created Kafka cluster")
        )),
        MetaApiError
    )
)]
pub async fn create_kafka_cluster<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Json(payload): Json<CreateKafkaClusterRequest>,
) -> Result<impl axum::response::IntoResponse, MetaApiError>
where
    Metadata: MetadataService,
{
    let cluster = state
        .schema_registry
        .create_kafka_cluster(payload.name, payload.properties)
        .await
        .inspect_err(|e| warn_it!(e))?;

    Ok((
        StatusCode::CREATED,
        [(
            http::header::LOCATION,
            format!("kafka-clusters/{}", cluster.name()),
        )],
        Json(SimpleKafkaClusterResponse::from(cluster)),
    ))
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct GetKafkaClusterParams {
    /// If true, includes the list of subscriptions using this cluster.
    #[serde(default)]
    pub include_subscriptions: bool,
}

/// Get Kafka cluster
///
/// Returns the details of a specific Kafka cluster, including its configuration properties.
/// Sensitive properties (passwords, secrets, etc.) are automatically redacted in the response.
#[utoipa::path(
    get,
    path = "/kafka-clusters/{cluster_name}",
    operation_id = "get_kafka_cluster",
    tag = "kafka_cluster",
    params(
        ("cluster_name" = String, Path, description = "Kafka cluster name"),
        GetKafkaClusterParams
    ),
    responses(
        (status = 200, description = "Kafka cluster details", body = KafkaClusterResponse),
        MetaApiError
    )
)]
pub async fn get_kafka_cluster<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(cluster_name): Path<String>,
    Query(GetKafkaClusterParams {
        include_subscriptions,
    }): Query<GetKafkaClusterParams>,
) -> Result<Json<KafkaClusterResponse>, MetaApiError>
where
    Metadata: MetadataService,
{
    Ok(Json(if include_subscriptions {
        let (cluster, subscriptions) = state
            .schema_registry
            .get_kafka_cluster_and_subscriptions(&cluster_name)
            .ok_or_else(|| MetaApiError::KafkaClusterNotFound(cluster_name.clone()))?;

        KafkaClusterResponse {
            name: cluster.name().to_string(),
            properties: cluster.properties().clone(),
            created_at: SystemTime::from(cluster.created_at).into(),
            subscriptions: subscriptions
                .into_iter()
                .map(SubscriptionResponse::from)
                .collect(),
        }
    } else {
        let cluster = state
            .schema_registry
            .get_kafka_cluster(&cluster_name)
            .ok_or_else(|| MetaApiError::KafkaClusterNotFound(cluster_name.clone()))?;

        KafkaClusterResponse {
            name: cluster.name().to_string(),
            properties: cluster.properties().clone(),
            created_at: SystemTime::from(cluster.created_at).into(),
            subscriptions: vec![],
        }
    }))
}

/// List Kafka clusters
///
/// Returns a list of all registered Kafka clusters.
#[utoipa::path(
    get,
    path = "/kafka-clusters",
    operation_id = "list_kafka_clusters",
    tag = "kafka_cluster",
    responses(
        (status = 200, description = "List of all Kafka clusters", body = ListKafkaClustersResponse)
    )
)]
pub async fn list_kafka_clusters<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
) -> Json<ListKafkaClustersResponse>
where
    Metadata: MetadataService,
{
    let clusters = state.schema_registry.get_kafka_clusters();

    ListKafkaClustersResponse {
        clusters: clusters
            .into_iter()
            .map(SimpleKafkaClusterResponse::from)
            .collect(),
    }
    .into()
}

/// Update Kafka cluster
///
/// Updates the configuration properties of an existing Kafka cluster.
/// Sensitive properties (passwords, secrets, etc.) are automatically redacted in the response.
#[utoipa::path(
    patch,
    path = "/kafka-clusters/{cluster_name}",
    operation_id = "update_kafka_cluster",
    tag = "kafka_cluster",
    params(
        ("cluster_name" = String, Path, description = "Kafka cluster name"),
    ),
    request_body = UpdateKafkaClusterRequest,
    responses(
        (status = 200, description = "Kafka cluster updated successfully", body = SimpleKafkaClusterResponse),
        MetaApiError
    )
)]
pub async fn update_kafka_cluster<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(cluster_name): Path<String>,
    Json(payload): Json<UpdateKafkaClusterRequest>,
) -> Result<Json<SimpleKafkaClusterResponse>, MetaApiError>
where
    Metadata: MetadataService,
{
    let cluster_name = cluster_name
        .parse()
        .map_err(|e| MetaApiError::InvalidField("cluster_name", format!("{}", e)))?;

    let cluster = state
        .schema_registry
        .update_kafka_cluster(cluster_name, payload.properties)
        .await
        .inspect_err(|e| warn_it!(e))?;

    Ok(SimpleKafkaClusterResponse::from(cluster).into())
}

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct DeleteKafkaClusterParams {
    /// If true, allows deletion of the cluster even if it would orphan subscriptions.
    pub force: Option<bool>,
}

/// Delete Kafka cluster
///
/// Deletes a Kafka cluster. By default, deletion is prevented if subscriptions reference the cluster.
/// Use the force parameter to allow deletion with orphaned subscriptions.
#[utoipa::path(
    delete,
    path = "/kafka-clusters/{cluster_name}",
    operation_id = "delete_kafka_cluster",
    tag = "kafka_cluster",
    params(
        ("cluster_name" = String, Path, description = "Kafka cluster name"),
        DeleteKafkaClusterParams
    ),
    responses(
        (status = 202, description = "Kafka cluster deletion accepted and will be processed asynchronously"),
        MetaApiError
    )
)]
pub async fn delete_kafka_cluster<Metadata, Discovery, Telemetry, Invocations, Transport>(
    State(state): State<AdminServiceState<Metadata, Discovery, Telemetry, Invocations, Transport>>,
    Path(cluster_name): Path<String>,
    Query(DeleteKafkaClusterParams { force }): Query<DeleteKafkaClusterParams>,
) -> Result<StatusCode, MetaApiError>
where
    Metadata: MetadataService,
{
    let cluster_name = cluster_name
        .parse()
        .map_err(|e| MetaApiError::InvalidField("cluster_name", format!("{}", e)))?;

    let allow_orphan = if force == Some(true) {
        AllowOrphanSubscriptions::Yes
    } else {
        AllowOrphanSubscriptions::No
    };

    state
        .schema_registry
        .delete_kafka_cluster(cluster_name, allow_orphan)
        .await
        .inspect_err(|e| warn_it!(e))?;

    Ok(StatusCode::ACCEPTED)
}
