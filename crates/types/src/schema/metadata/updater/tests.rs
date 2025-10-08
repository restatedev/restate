// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use std::convert::Infallible;

use crate::Versioned;
use crate::schema::deployment::DeploymentResolver;
use crate::schema::deployment::ProtocolType;
use crate::schema::invocation_target::InvocationTargetResolver;
use crate::schema::service::ServiceMetadataResolver;
use crate::service_protocol::{
    MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION, MIN_INFLIGHT_SERVICE_PROTOCOL_VERSION,
};
use http::HeaderName;
use restate_test_util::{assert, assert_eq};
use test_log::test;

const GREETER_SERVICE_NAME: &str = "greeter.Greeter";
const GREET_HANDLER_NAME: &str = "greet";
const ANOTHER_GREETER_SERVICE_NAME: &str = "greeter.AnotherGreeter";

fn greeter_service_greet_handler() -> endpoint_manifest::Handler {
    endpoint_manifest::Handler {
        abort_timeout: None,
        documentation: None,
        idempotency_retention: None,
        name: GREET_HANDLER_NAME.parse().unwrap(),
        ty: None,
        input: None,
        output: None,
        retry_policy_exponentiation_factor: None,
        retry_policy_initial_interval: None,
        retry_policy_max_attempts: None,
        retry_policy_max_interval: None,
        metadata: Default::default(),
        inactivity_timeout: None,
        journal_retention: None,
        workflow_completion_retention: None,
        enable_lazy_state: None,
        ingress_private: None,
        retry_policy_on_max_attempts: None,
    }
}

fn greeter_workflow_greet_handler() -> endpoint_manifest::Handler {
    endpoint_manifest::Handler {
        abort_timeout: None,
        documentation: None,
        idempotency_retention: None,
        name: "greet".parse().unwrap(),
        ty: Some(endpoint_manifest::HandlerType::Workflow),
        input: None,
        output: None,
        retry_policy_exponentiation_factor: None,
        retry_policy_initial_interval: None,
        retry_policy_max_attempts: None,
        retry_policy_max_interval: None,
        metadata: Default::default(),
        inactivity_timeout: None,
        journal_retention: None,
        workflow_completion_retention: None,
        enable_lazy_state: None,
        ingress_private: None,
        retry_policy_on_max_attempts: None,
    }
}

fn greeter_service() -> endpoint_manifest::Service {
    endpoint_manifest::Service {
        abort_timeout: None,
        documentation: None,
        ingress_private: None,
        ty: endpoint_manifest::ServiceType::Service,
        name: GREETER_SERVICE_NAME.parse().unwrap(),
        retry_policy_exponentiation_factor: None,
        retry_policy_initial_interval: None,
        retry_policy_max_attempts: None,
        retry_policy_max_interval: None,
        handlers: vec![greeter_service_greet_handler()],
        idempotency_retention: None,
        inactivity_timeout: None,
        journal_retention: None,
        metadata: Default::default(),
        enable_lazy_state: None,
        retry_policy_on_max_attempts: None,
    }
}

fn greeter_virtual_object() -> endpoint_manifest::Service {
    endpoint_manifest::Service {
        abort_timeout: None,
        documentation: None,
        ingress_private: None,
        ty: endpoint_manifest::ServiceType::VirtualObject,
        name: GREETER_SERVICE_NAME.parse().unwrap(),
        retry_policy_exponentiation_factor: None,
        retry_policy_initial_interval: None,
        retry_policy_max_attempts: None,
        retry_policy_max_interval: None,
        handlers: vec![endpoint_manifest::Handler {
            abort_timeout: None,
            documentation: None,
            idempotency_retention: None,
            name: "greet".parse().unwrap(),
            ty: None,
            input: None,
            output: None,
            retry_policy_exponentiation_factor: None,
            retry_policy_initial_interval: None,
            retry_policy_max_attempts: None,
            retry_policy_max_interval: None,
            metadata: Default::default(),
            inactivity_timeout: None,
            journal_retention: None,
            workflow_completion_retention: None,
            enable_lazy_state: None,
            ingress_private: None,
            retry_policy_on_max_attempts: None,
        }],
        idempotency_retention: None,
        inactivity_timeout: None,
        journal_retention: None,
        metadata: Default::default(),
        enable_lazy_state: None,
        retry_policy_on_max_attempts: None,
    }
}

fn greeter_workflow() -> endpoint_manifest::Service {
    endpoint_manifest::Service {
        abort_timeout: None,
        documentation: None,
        ingress_private: None,
        ty: endpoint_manifest::ServiceType::Workflow,
        name: GREETER_SERVICE_NAME.parse().unwrap(),
        retry_policy_exponentiation_factor: None,
        retry_policy_initial_interval: None,
        retry_policy_max_attempts: None,
        retry_policy_max_interval: None,
        handlers: vec![greeter_workflow_greet_handler()],
        idempotency_retention: None,
        inactivity_timeout: None,
        journal_retention: None,
        metadata: Default::default(),
        enable_lazy_state: None,
        retry_policy_on_max_attempts: None,
    }
}

fn another_greeter_service() -> endpoint_manifest::Service {
    endpoint_manifest::Service {
        abort_timeout: None,
        documentation: None,
        ingress_private: None,
        ty: endpoint_manifest::ServiceType::Service,
        name: ANOTHER_GREETER_SERVICE_NAME.parse().unwrap(),
        retry_policy_exponentiation_factor: None,
        retry_policy_initial_interval: None,
        retry_policy_max_attempts: None,
        retry_policy_max_interval: None,
        handlers: vec![endpoint_manifest::Handler {
            abort_timeout: None,
            documentation: None,
            idempotency_retention: None,
            name: "another_greeter".parse().unwrap(),
            ty: None,
            input: None,
            output: None,
            retry_policy_exponentiation_factor: None,
            retry_policy_initial_interval: None,
            retry_policy_max_attempts: None,
            retry_policy_max_interval: None,
            metadata: Default::default(),
            inactivity_timeout: None,
            journal_retention: None,
            workflow_completion_retention: None,
            enable_lazy_state: None,
            ingress_private: None,
            retry_policy_on_max_attempts: None,
        }],
        idempotency_retention: None,
        inactivity_timeout: None,
        journal_retention: None,
        metadata: Default::default(),
        enable_lazy_state: None,
        retry_policy_on_max_attempts: None,
    }
}

fn add_deployment_request(services: Vec<endpoint_manifest::Service>) -> AddDeploymentRequest {
    AddDeploymentRequest {
        deployment_address: DeploymentAddress::mock(),
        additional_headers: Default::default(),
        metadata: Default::default(),
        discovery_response: DiscoveryResponse {
            deployment_type_parameters: DeploymentConnectionParameters::Http {
                protocol_type: ProtocolType::BidiStream,
                http_version: http::Version::HTTP_2,
            },
            supported_protocol_versions: (MIN_INFLIGHT_SERVICE_PROTOCOL_VERSION as i32)
                ..=(MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION as i32),
            sdk_version: None,
            services,
        },
        allow_breaking_changes: AllowBreakingChanges::No,
        overwrite: Overwrite::No,
    }
}

fn update_deployment_request(
    deployment_id: DeploymentId,
    services: Vec<endpoint_manifest::Service>,
) -> UpdateDeploymentRequest {
    UpdateDeploymentRequest {
        deployment_id,
        deployment_address: DeploymentAddress::mock(),
        additional_headers: Default::default(),
        discovery_response: DiscoveryResponse {
            deployment_type_parameters: DeploymentConnectionParameters::Http {
                protocol_type: ProtocolType::BidiStream,
                http_version: http::Version::HTTP_2,
            },
            supported_protocol_versions: (MIN_INFLIGHT_SERVICE_PROTOCOL_VERSION as i32)
                ..=(MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION as i32),
            sdk_version: None,
            services,
        },
        overwrite: Overwrite::No,
    }
}

#[test]
fn register_new_deployment() {
    let schema = Schema::default();
    let initial_version = schema.version();

    let ((result, new_deployment_id), schema) =
        SchemaUpdater::update_and_return(schema, |updater| {
            updater.add_deployment(add_deployment_request(vec![greeter_service()]))
        })
        .unwrap();

    assert_eq!(result, AddDeploymentResult::Created);
    assert!(initial_version < schema.version());
    schema.assert_service_revision(GREETER_SERVICE_NAME, 1);
    schema.assert_service_deployment(GREETER_SERVICE_NAME, new_deployment_id);
    schema.assert_invocation_target(GREETER_SERVICE_NAME, "greet");
}

#[test]
fn register_new_deployment_add_unregistered_service() {
    let mut updater = SchemaUpdater::default();

    // Register first deployment
    let deployment_1 = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1);
    assert!(
        schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none()
    );

    updater = SchemaUpdater::new(schemas);
    let deployment_2 = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_service(), another_greeter_service()])
        })
        .unwrap()
        .1;
    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_2);
    schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
}

mod routing_header {
    use super::*;

    use restate_test_util::assert_eq;
    use test_log::test;

    #[test]
    fn register_new_deployment_with_routing_header_first() {
        let mut config = Configuration::default();
        config.admin.deployment_routing_headers = vec![HeaderName::from_static("x-routing")];
        crate::config::set_current_config(config);

        // Register first deployment
        let ((result, deployment_id_1), schema) =
            SchemaUpdater::update_and_return(Schema::default(), |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [(
                        HeaderName::from_static("x-routing"),
                        HeaderValue::from_static("1"),
                    )]
                    .into(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_1);

        // Update providing another routing header
        let ((result, deployment_id_2), schema) =
            SchemaUpdater::update_and_return(schema, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [(
                        HeaderName::from_static("x-routing"),
                        HeaderValue::from_static("2"),
                    )]
                    .into(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);

        // Update not providing routing_header -> new deployment here
        let ((result, deployment_id_3), schema) =
            SchemaUpdater::update_and_return(schema, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: Default::default(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_3);

        // Update providing the same routing header-> conflict
        let ((result, expected_dp_id_2), schema) =
            SchemaUpdater::update_and_return(schema.clone(), |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [(
                        HeaderName::from_static("x-routing"),
                        HeaderValue::from_static("2"),
                    )]
                    .into(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Unchanged, result);
        assert_eq!(expected_dp_id_2, deployment_id_2);

        // Force with same routing header -> all good
        let ((result, expected_dp_id_2), schema) =
            SchemaUpdater::update_and_return(schema, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [(
                        HeaderName::from_static("x-routing"),
                        HeaderValue::from_static("2"),
                    )]
                    .into(),
                    allow_breaking_changes: AllowBreakingChanges::Yes,
                    overwrite: Overwrite::Yes,
                    ..add_deployment_request(vec![greeter_virtual_object()])
                })
            })
            .unwrap();
        assert_eq!(expected_dp_id_2, deployment_id_2);
        assert_eq!(AddDeploymentResult::Overwritten, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);
        assert_eq!(
            schema.assert_service(GREETER_SERVICE_NAME).ty,
            ServiceType::VirtualObject
        );
    }

    #[test]
    fn register_new_deployment_without_routing_header_first() {
        let mut config = Configuration::default();
        config.admin.deployment_routing_headers = vec![HeaderName::from_static("x-routing")];
        crate::config::set_current_config(config);

        // Register first deployment with routing header
        let ((result, deployment_id_1), schema) =
            SchemaUpdater::update_and_return(Schema::default(), |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [(
                        HeaderName::from_static("x-routing"),
                        HeaderValue::from_static("1"),
                    )]
                    .into(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_1);

        // Update without routing header -> new deployment
        let ((result, deployment_id_2), schema) =
            SchemaUpdater::update_and_return(schema, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: Default::default(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);
    }

    #[test]
    fn with_multiple_configured_routing_headers() {
        let mut config = Configuration::default();
        config.admin.deployment_routing_headers = vec![
            HeaderName::from_static("x-restate-routing"),
            HeaderName::from_static("x-my-routing"),
        ];
        crate::config::set_current_config(config);

        // Register first deployment with routing header
        let ((result, deployment_id_1), schema) =
            SchemaUpdater::update_and_return(Schema::default(), |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [(
                        HeaderName::from_static("x-restate-routing"),
                        HeaderValue::from_static("1"),
                    )]
                    .into(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_1);

        // Update with different routing header -> new deployment
        let ((result, deployment_id_2), schema) =
            SchemaUpdater::update_and_return(schema, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [(
                        HeaderName::from_static("x-my-routing"),
                        HeaderValue::from_static("1"),
                    )]
                    .into(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);

        // No routing header -> new deployment
        let ((result, deployment_id_3), schema) =
            SchemaUpdater::update_and_return(schema, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: Default::default(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_3);

        // Update with both routing header -> new deployment
        let ((result, deployment_id_4), schema) =
            SchemaUpdater::update_and_return(schema, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [
                        (
                            HeaderName::from_static("x-restate-routing"),
                            HeaderValue::from_static("1"),
                        ),
                        (
                            HeaderName::from_static("x-my-routing"),
                            HeaderValue::from_static("2"),
                        ),
                    ]
                    .into(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Created, result);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_4);

        // Update with same header -> fails
        let ((result, expected_dp_id_2), schema) =
            SchemaUpdater::update_and_return(schema, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    additional_headers: [(
                        HeaderName::from_static("x-my-routing"),
                        HeaderValue::from_static("1"),
                    )]
                    .into(),
                    ..add_deployment_request(vec![greeter_service()])
                })
            })
            .unwrap();
        assert_eq!(AddDeploymentResult::Unchanged, result);
        assert_eq!(expected_dp_id_2, deployment_id_2);

        // Update without header -> fails
        let ((result, expected_dp_id_3), _) = SchemaUpdater::update_and_return(schema, |updater| {
            updater.add_deployment(add_deployment_request(vec![greeter_service()]))
        })
        .unwrap();
        assert_eq!(AddDeploymentResult::Unchanged, result);
        assert_eq!(expected_dp_id_3, deployment_id_3);
    }
}

/// This test case ensures that https://github.com/restatedev/restate/issues/1205 works
#[test]
fn force_deploy_private_service() -> Result<(), SchemaError> {
    let mut updater = SchemaUpdater::default();

    let deployment_id = updater
        .add_deployment(add_deployment_request(vec![greeter_service()]))?
        .1;

    let schemas = updater.into_inner();

    assert!(schemas.assert_service(GREETER_SERVICE_NAME).public);
    assert!(
        schemas
            .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
            .public
    );

    let version_before_modification = schemas.version();
    updater = SchemaUpdater::new(schemas);
    updater.modify_service(
        GREETER_SERVICE_NAME,
        vec![ModifyServiceChange::Public(false)],
    )?;
    let schemas = updater.into_inner();

    assert!(version_before_modification < schemas.version());
    assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
    assert!(
        !schemas
            .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
            .public
    );

    updater = SchemaUpdater::new(schemas);
    let new_deployment_id = updater
        .add_deployment(AddDeploymentRequest {
            allow_breaking_changes: AllowBreakingChanges::Yes,
            overwrite: Overwrite::Yes,
            ..add_deployment_request(vec![greeter_service()])
        })?
        .1;

    let schemas = updater.into_inner();
    assert_eq!(deployment_id, new_deployment_id);
    assert!(schemas.assert_service(GREETER_SERVICE_NAME).public);
    assert!(
        schemas
            .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
            .public
    );

    Ok(())
}

#[test]
fn register_new_deployment_allow_breaking_changes() {
    let mut updater = SchemaUpdater::default();

    // Register first deployment
    let deployment_1_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1_id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);
    assert_eq!(
        schemas.assert_service(GREETER_SERVICE_NAME).ty,
        ServiceType::Service
    );

    updater = SchemaUpdater::new(schemas);
    let deployment_2_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            allow_breaking_changes: AllowBreakingChanges::Yes,
            ..add_deployment_request(vec![greeter_virtual_object()])
        })
        .unwrap()
        .1;
    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2_id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    assert_eq!(
        schemas.assert_service(GREETER_SERVICE_NAME).ty,
        ServiceType::VirtualObject
    );
}

mod change_service_type {
    use super::*;

    use restate_test_util::{assert, assert_eq};
    use test_log::test;

    #[test]
    fn fails_without_allow_breaking_changes() {
        let mut updater = SchemaUpdater::default();

        let deployment_1_id = updater
            .add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
                ..add_deployment_request(vec![greeter_service()])
            })
            .unwrap()
            .1;
        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1_id);

        let compute_result = SchemaUpdater::new(schemas).add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_virtual_object()])
        });

        assert!(let &SchemaError::Service(
                ServiceError::DifferentType(_)
            ) = compute_result.unwrap_err());
    }

    #[test]
    fn works_with_allow_breaking_changes() {
        let mut updater = SchemaUpdater::default();

        let deployment_1_id = updater
            .add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
                ..add_deployment_request(vec![greeter_service()])
            })
            .unwrap()
            .1;
        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1_id);

        let ((result, deployment_id_2), schemas) =
            SchemaUpdater::update_and_return(schemas, |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
                    allow_breaking_changes: AllowBreakingChanges::Yes,
                    ..add_deployment_request(vec![greeter_virtual_object()])
                })
            })
            .unwrap();
        assert_eq!(result, AddDeploymentResult::Created);
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);

        assert_eq!(
            schemas.assert_service(GREETER_SERVICE_NAME).ty,
            ServiceType::VirtualObject
        );
    }

    #[test]
    fn works_with_allow_breaking_changed_and_correctly_set_workflow_retention() {
        let mut updater = SchemaUpdater::default();

        let deployment_1_id = updater
            .add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
                ..add_deployment_request(vec![greeter_service()])
            })
            .unwrap()
            .1;
        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1_id);

        updater = SchemaUpdater::new(schemas);
        let deployment_2_id = updater
            .add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
                allow_breaking_changes: AllowBreakingChanges::Yes,
                ..add_deployment_request(vec![greeter_workflow()])
            })
            .unwrap()
            .1;
        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2_id);

        let new_svc = schemas.assert_service(GREETER_SERVICE_NAME);
        assert_eq!(new_svc.ty, ServiceType::Workflow);
        assert_eq!(
            new_svc.workflow_completion_retention,
            Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)
        );
    }
}

#[test]
fn override_existing_deployment_removing_a_service() {
    let ((_, deployment_id), schemas) =
        SchemaUpdater::update_and_return(Schema::default(), |updater| {
            updater.add_deployment(add_deployment_request(vec![
                greeter_service(),
                another_greeter_service(),
            ]))
        })
        .unwrap();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id);
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_id);

    let ((result, new_deployment_id), schemas) =
        SchemaUpdater::update_and_return(schemas, |updater| {
            updater.add_deployment(AddDeploymentRequest {
                allow_breaking_changes: AllowBreakingChanges::Yes,
                overwrite: Overwrite::Yes,
                ..add_deployment_request(vec![greeter_service()])
            })
        })
        .unwrap();
    assert_eq!(result, AddDeploymentResult::Overwritten);
    assert_eq!(new_deployment_id, deployment_id);

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id);
    assert!(
        schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none()
    );
}

#[test]
fn idempotent_add() {
    let mut updater = SchemaUpdater::default();

    let deployment_id = updater
        .add_deployment(add_deployment_request(vec![greeter_service()]))
        .unwrap()
        .1;

    // Id didn't change
    assert_eq!(
        updater
            .add_deployment(add_deployment_request(vec![greeter_service()]))
            .unwrap()
            .1,
        deployment_id
    );
}

#[test]
fn register_two_deployments_then_remove_first() {
    let ((_, deployment_id_1), schemas) =
        SchemaUpdater::update_and_return(Schema::default(), |updater| {
            updater.add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
                ..add_deployment_request(vec![greeter_service(), another_greeter_service()])
            })
        })
        .unwrap();

    let ((_, deployment_id_2), schemas) = SchemaUpdater::update_and_return(schemas, |updater| {
        updater.add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_service()])
        })
    })
    .unwrap();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_id_1);
    schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);

    let version_before_removal = schemas.version();
    let schemas = SchemaUpdater::update(schemas, |updater| {
        assert!(updater.remove_deployment(deployment_id_1));
        Ok::<(), Infallible>(())
    })
    .unwrap();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    assert!(version_before_removal < schemas.version());
    assert!(
        schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none()
    );
    assert!(schemas.get_deployment(&deployment_id_1).is_none());
}

#[test]
fn register_two_deployments_then_remove_second() {
    let ((_, deployment_id_1), schemas) =
        SchemaUpdater::update_and_return(Schema::default(), |updater| {
            updater.add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
                ..add_deployment_request(vec![greeter_service(), another_greeter_service()])
            })
        })
        .unwrap();

    let ((_, deployment_id_2), schemas) = SchemaUpdater::update_and_return(schemas, |updater| {
        updater.add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_service()])
        })
    })
    .unwrap();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_id_1);
    schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);

    let version_before_removal = schemas.version();
    let schemas = SchemaUpdater::update(schemas, |updater| {
        assert!(updater.remove_deployment(deployment_id_2));
        Ok::<(), Infallible>(())
    })
    .unwrap();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_1);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);
    assert!(version_before_removal < schemas.version());
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_id_1);
    schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    assert!(schemas.get_deployment(&deployment_id_2).is_none());
}

mod remove_handler {
    use super::*;

    use restate_test_util::{check, let_assert};
    use test_log::test;

    fn greeter_v1_service() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            abort_timeout: None,
            documentation: None,
            ingress_private: None,
            ty: endpoint_manifest::ServiceType::Service,
            name: GREETER_SERVICE_NAME.parse().unwrap(),
            retry_policy_exponentiation_factor: None,
            retry_policy_initial_interval: None,
            retry_policy_max_attempts: None,
            retry_policy_max_interval: None,
            handlers: vec![
                endpoint_manifest::Handler {
                    abort_timeout: None,
                    documentation: None,
                    idempotency_retention: None,
                    name: "greet".parse().unwrap(),
                    ty: None,
                    input: None,
                    output: None,
                    retry_policy_exponentiation_factor: None,
                    retry_policy_initial_interval: None,
                    retry_policy_max_attempts: None,
                    retry_policy_max_interval: None,
                    metadata: Default::default(),
                    inactivity_timeout: None,
                    journal_retention: None,
                    workflow_completion_retention: None,
                    enable_lazy_state: None,
                    ingress_private: None,
                    retry_policy_on_max_attempts: None,
                },
                endpoint_manifest::Handler {
                    abort_timeout: None,
                    documentation: None,
                    idempotency_retention: None,
                    name: "doSomething".parse().unwrap(),
                    ty: None,
                    input: None,
                    output: None,
                    retry_policy_exponentiation_factor: None,
                    retry_policy_initial_interval: None,
                    retry_policy_max_attempts: None,
                    retry_policy_max_interval: None,
                    metadata: Default::default(),
                    inactivity_timeout: None,
                    journal_retention: None,
                    workflow_completion_retention: None,
                    enable_lazy_state: None,
                    ingress_private: None,
                    retry_policy_on_max_attempts: None,
                },
            ],
            idempotency_retention: None,
            inactivity_timeout: None,
            journal_retention: None,
            metadata: Default::default(),
            enable_lazy_state: None,
            retry_policy_on_max_attempts: None,
        }
    }

    fn greeter_v2_service() -> endpoint_manifest::Service {
        endpoint_manifest::Service {
            abort_timeout: None,
            documentation: None,
            ingress_private: None,
            ty: endpoint_manifest::ServiceType::Service,
            name: GREETER_SERVICE_NAME.parse().unwrap(),
            retry_policy_exponentiation_factor: None,
            retry_policy_initial_interval: None,
            retry_policy_max_attempts: None,
            retry_policy_max_interval: None,
            handlers: vec![endpoint_manifest::Handler {
                abort_timeout: None,
                documentation: None,
                idempotency_retention: None,
                name: "greet".parse().unwrap(),
                ty: None,
                input: None,
                output: None,
                retry_policy_exponentiation_factor: None,
                retry_policy_initial_interval: None,
                retry_policy_max_attempts: None,
                retry_policy_max_interval: None,
                metadata: Default::default(),
                inactivity_timeout: None,
                journal_retention: None,
                workflow_completion_retention: None,
                enable_lazy_state: None,
                ingress_private: None,
                retry_policy_on_max_attempts: None,
            }],
            idempotency_retention: None,
            inactivity_timeout: None,
            journal_retention: None,
            metadata: Default::default(),
            enable_lazy_state: None,
            retry_policy_on_max_attempts: None,
        }
    }

    #[test]
    fn reject_removing_existing_methods() {
        let mut updater = SchemaUpdater::default();

        updater
            .add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
                ..add_deployment_request(vec![greeter_v1_service()])
            })
            .unwrap();
        let schemas = updater.into_inner();
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);

        updater = SchemaUpdater::new(schemas);
        let rejection = updater
            .add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
                ..add_deployment_request(vec![greeter_v2_service()])
            })
            .unwrap_err();

        let schemas = updater.into_inner();
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 1); // unchanged

        let_assert!(
            SchemaError::Service(ServiceError::RemovedHandlers(service, missing_methods)) =
                rejection
        );
        check!(service == GREETER_SERVICE_NAME);
        check!(missing_methods == &["doSomething"]);
    }
}

#[test]
fn update_latest_deployment() {
    let mut updater = SchemaUpdater::default();

    let deployment_1_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            ..add_deployment_request(vec![greeter_virtual_object()])
        })
        .unwrap()
        .1;

    assert!(let &SchemaError::NotFound(_) = updater.update_deployment(
            UpdateDeploymentRequest {
                overwrite: Overwrite::Yes,
                ..update_deployment_request(DeploymentId::new(), vec![])
            }
        ).unwrap_err());

    updater
        .update_deployment(UpdateDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            additional_headers: [(
                HeaderName::from_static("foo"),
                HeaderValue::from_static("bar"),
            )]
            .into(),
            ..update_deployment_request(deployment_1_id, vec![greeter_virtual_object()])
        })
        .unwrap();

    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1_id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);

    let updated_deployment = schemas.get_deployment(&deployment_1_id).unwrap();

    assert!(
        updated_deployment
            .additional_headers
            .contains_key(&HeaderName::from_static("foo"))
    );
}

#[test]
fn update_draining_deployment() {
    let mut updater = SchemaUpdater::default();

    let deployment_1_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    let deployment_2_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_service(), another_greeter_service()])
        })
        .unwrap()
        .1;

    updater
        .update_deployment(UpdateDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            additional_headers: [(
                HeaderName::from_static("foo"),
                HeaderValue::from_static("bar"),
            )]
            .into(),
            overwrite: Overwrite::Yes,
            ..update_deployment_request(deployment_1_id, vec![greeter_service()])
        })
        .unwrap();

    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2_id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);

    let updated_deployment = schemas.get_deployment(&deployment_1_id).unwrap();

    assert!(
        updated_deployment
            .additional_headers
            .contains_key(&HeaderName::from_static("foo"))
    );
}

#[test]
fn update_deployment_same_uri() {
    let ((_, deployment_id_1), schemas) =
        SchemaUpdater::update_and_return(Schema::default(), |updater| {
            updater.add_deployment(AddDeploymentRequest {
                deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
                ..add_deployment_request(vec![greeter_service()])
            })
        })
        .unwrap();

    // patching new invocations
    let ((_, deployment_id_2), schemas) = SchemaUpdater::update_and_return(schemas, |updater| {
        updater.add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_service()])
        })
    })
    .unwrap();

    // oh, I have some old failing invocations, wish those were on the patched version too

    let schemas = SchemaUpdater::update(schemas, |updater| {
        updater.update_deployment(UpdateDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..update_deployment_request(deployment_id_1, vec![greeter_service()])
        })
    })
    .unwrap();

    // Latest should remain deployment_2
    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);

    let updated_deployment_1 = schemas.get_deployment(&deployment_id_1).unwrap();
    let updated_deployment_2 = schemas.get_deployment(&deployment_id_2).unwrap();

    assert_eq!(updated_deployment_1.ty, updated_deployment_2.ty);

    // the failing invocations have drained so I can safely delete the original deployment
    let schemas = SchemaUpdater::update(schemas, |updater| {
        assert!(updater.remove_deployment(deployment_id_1));
        Ok::<(), Infallible>(())
    })
    .unwrap();

    assert_eq!(
        deployment_id_2,
        SchemaUpdater::update_and_return(schemas, |updater| updater.add_deployment(
            AddDeploymentRequest {
                allow_breaking_changes: AllowBreakingChanges::Yes,
                overwrite: Overwrite::Yes,
                ..add_deployment_request(vec![greeter_service(), greeter_virtual_object()])
            }
        ))
        .unwrap()
        .0
        .1
    );
}

#[test]
fn update_latest_deployment_add_handler() {
    let mut updater = SchemaUpdater::default();

    let deployment_1_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    let mut updated_greeter_service = greeter_service();
    updated_greeter_service
        .handlers
        .push(endpoint_manifest::Handler {
            abort_timeout: None,
            documentation: None,
            idempotency_retention: None,
            name: "greetAgain".parse().unwrap(),
            ty: None,
            input: None,
            output: None,
            retry_policy_exponentiation_factor: None,
            retry_policy_initial_interval: None,
            retry_policy_max_attempts: None,
            retry_policy_max_interval: None,
            metadata: Default::default(),
            inactivity_timeout: None,
            journal_retention: None,
            workflow_completion_retention: None,
            enable_lazy_state: None,
            ingress_private: None,
            retry_policy_on_max_attempts: None,
        });

    updater
        .update_deployment(UpdateDeploymentRequest {
            overwrite: Overwrite::Yes,
            ..update_deployment_request(deployment_1_id, vec![updated_greeter_service])
        })
        .unwrap();

    updater
        .schema
        .assert_service_deployment(GREETER_SERVICE_NAME, deployment_1_id);
    updater
        .schema
        .assert_service_revision(GREETER_SERVICE_NAME, 1);

    let (_, services) = updater
        .schema
        .get_deployment_and_services(&deployment_1_id)
        .unwrap();

    assert_eq!(
        services
            .iter()
            .find(|s| s.name == GREETER_SERVICE_NAME)
            .unwrap()
            .handlers
            .len(),
        2
    );
}

#[test]
fn update_draining_deployment_add_handler() {
    let mut updater = SchemaUpdater::default();

    let deployment_1_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    let deployment_2_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    let mut updated_greeter_service = greeter_service();
    updated_greeter_service
        .handlers
        .push(endpoint_manifest::Handler {
            abort_timeout: None,
            documentation: None,
            idempotency_retention: None,
            name: "greetAgain".parse().unwrap(),
            ty: None,
            input: None,
            output: None,
            retry_policy_exponentiation_factor: None,
            retry_policy_initial_interval: None,
            retry_policy_max_attempts: None,
            retry_policy_max_interval: None,
            metadata: Default::default(),
            inactivity_timeout: None,
            journal_retention: None,
            workflow_completion_retention: None,
            enable_lazy_state: None,
            ingress_private: None,
            retry_policy_on_max_attempts: None,
        });

    updater
        .update_deployment(UpdateDeploymentRequest {
            overwrite: Overwrite::Yes,
            ..update_deployment_request(deployment_1_id, vec![updated_greeter_service])
        })
        .unwrap();

    updater
        .schema
        .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2_id);
    updater
        .schema
        .assert_service_revision(GREETER_SERVICE_NAME, 2);

    let (_, updated_deployment_1_services) = updater
        .schema
        .get_deployment_and_services(&deployment_1_id)
        .unwrap();

    assert_eq!(
        updated_deployment_1_services
            .iter()
            .find(|s| s.name == GREETER_SERVICE_NAME)
            .unwrap()
            .handlers
            .len(),
        2
    );

    let (_, updated_deployment_2_services) = updater
        .schema
        .get_deployment_and_services(&deployment_2_id)
        .unwrap();

    assert_eq!(
        updated_deployment_2_services
            .iter()
            .find(|s| s.name == GREETER_SERVICE_NAME)
            .unwrap()
            .handlers
            .len(),
        1
    )
}

#[test]
fn update_latest_deployment_add_service() {
    let mut updater = SchemaUpdater::default();

    let deployment_1_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    updater
        .update_deployment(UpdateDeploymentRequest {
            overwrite: Overwrite::Yes,
            ..update_deployment_request(
                deployment_1_id,
                vec![greeter_service(), another_greeter_service()],
            )
        })
        .unwrap();

    updater
        .schema
        .assert_service_deployment(GREETER_SERVICE_NAME, deployment_1_id);
    updater
        .schema
        .assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1_id);
    updater
        .schema
        .assert_service_revision(GREETER_SERVICE_NAME, 1);
    updater
        .schema
        .assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
}

#[test]
fn update_draining_deployment_add_service() {
    let mut updater = SchemaUpdater::default();

    let deployment_1_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9080"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    let deployment_2_id = updater
        .add_deployment(AddDeploymentRequest {
            deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
            ..add_deployment_request(vec![greeter_service()])
        })
        .unwrap()
        .1;

    updater
        .update_deployment(UpdateDeploymentRequest {
            overwrite: Overwrite::Yes,
            ..update_deployment_request(
                deployment_1_id,
                vec![greeter_service(), another_greeter_service()],
            )
        })
        .unwrap();

    updater
        .schema
        .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2_id);
    updater
        .schema
        .assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1_id);
    updater
        .schema
        .assert_service_revision(GREETER_SERVICE_NAME, 2);
    updater
        .schema
        .assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
}

#[test]
fn update_deployment_with_private_service() -> Result<(), SchemaError> {
    let mut updater = SchemaUpdater::default();

    let deployment_id = updater
        .add_deployment(add_deployment_request(vec![greeter_service()]))?
        .1;

    let schemas = updater.into_inner();

    assert!(schemas.assert_service(GREETER_SERVICE_NAME).public);
    assert!(
        schemas
            .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
            .public
    );

    let version_before_modification = schemas.version();
    updater = SchemaUpdater::new(schemas);
    updater.modify_service(
        GREETER_SERVICE_NAME,
        vec![ModifyServiceChange::Public(false)],
    )?;
    let schemas = updater.into_inner();
    let version_after_modification = schemas.version();

    assert!(version_before_modification < version_after_modification);
    assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
    assert!(
        !schemas
            .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
            .public
    );

    updater = SchemaUpdater::new(schemas);
    updater.update_deployment(update_deployment_request(
        deployment_id,
        vec![greeter_service()],
    ))?;

    let schemas = updater.into_inner();
    assert!(version_before_modification < schemas.version());
    assert!(!schemas.assert_service(GREETER_SERVICE_NAME).public);
    assert!(
        !schemas
            .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
            .public
    );

    Ok(())
}

mod endpoint_manifest_options_propagation {
    use super::*;

    use crate::config::Configuration;
    use crate::invocation::InvocationRetention;
    use crate::schema::invocation_target::{InvocationAttemptOptions, InvocationTargetMetadata};
    use crate::schema::service::{HandlerMetadata, ServiceMetadata};
    use googletest::prelude::*;
    use restate_time_util::FriendlyDuration;
    use std::time::Duration;
    use test_log::test;

    fn init_discover_and_resolve_target(
        svc: endpoint_manifest::Service,
        service_name: &str,
        handler_name: &str,
    ) -> InvocationTargetMetadata {
        let ((_, deployment_id), schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(add_deployment_request(vec![svc]))
            })
            .unwrap();

        schema.assert_service_revision(service_name, 1);
        schema.assert_service_deployment(service_name, deployment_id);

        schema.assert_invocation_target(service_name, handler_name)
    }

    #[test]
    fn private_service() {
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                ingress_private: Some(true),
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(target.public, eq(false));
    }

    #[test]
    fn public_service_with_private_handler() {
        let schema = SchemaUpdater::update(Schema::default(), |updater| {
            updater
                .add_deployment(add_deployment_request(vec![endpoint_manifest::Service {
                    // Mock two handlers, one explicitly private, the other just default settings
                    handlers: vec![
                        endpoint_manifest::Handler {
                            ingress_private: Some(true),
                            name: "my_private_handler".parse().unwrap(),
                            ..greeter_service_greet_handler()
                        },
                        greeter_service_greet_handler(),
                    ],
                    ..greeter_service()
                }]))
                .map(|_| ())
        })
        .unwrap();

        // The explicitly private handler is private
        let private_handler_target =
            schema.assert_invocation_target(GREETER_SERVICE_NAME, "my_private_handler");
        assert_that!(private_handler_target.public, eq(false));

        // The other handler is by default public
        let public_handler_target =
            schema.assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME);
        assert_that!(public_handler_target.public, eq(true));
    }

    #[test]
    fn public_handler_in_private_service() {
        let schema_information = Schema::default();
        let mut updater = SchemaUpdater::new(schema_information);

        assert_that!(
            updater.add_deployment(add_deployment_request(vec![endpoint_manifest::Service {
                ingress_private: Some(true),
                handlers: vec![endpoint_manifest::Handler {
                    ingress_private: Some(false),
                    name: "my_private_handler".parse().unwrap(),
                    ..greeter_service_greet_handler()
                }],
                ..greeter_service()
            }])),
            err(pat!(SchemaError::Service(pat!(
                ServiceError::BadHandlerVisibility { .. }
            ))))
        );
    }

    #[test]
    fn workflow_retention() {
        let ((_, deployment_id), schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(add_deployment_request(vec![endpoint_manifest::Service {
                    handlers: vec![endpoint_manifest::Handler {
                        workflow_completion_retention: Some(30 * 1000),
                        ..greeter_workflow_greet_handler()
                    }],
                    ..greeter_workflow()
                }]))
            })
            .unwrap();

        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                revision: eq(1),
                deployment_id: eq(deployment_id),
                workflow_completion_retention: eq(Some(Duration::from_secs(30)))
            })
        );
        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(30),
                journal_retention: Duration::from_secs(30),
            })
        );
    }

    #[test]
    fn service_level_journal_retention() {
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(60 * 1000),
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(60),
                journal_retention: Duration::from_secs(60),
            })
        )
    }

    #[test]
    fn handler_level_journal_retention() {
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                handlers: vec![endpoint_manifest::Handler {
                    journal_retention: Some(30 * 1000),
                    ..greeter_service_greet_handler()
                }],
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(30),
                journal_retention: Duration::from_secs(30),
            })
        )
    }

    #[test]
    fn handler_level_journal_retention_overrides_the_service_level_journal_retention() {
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(60 * 1000),
                handlers: vec![endpoint_manifest::Handler {
                    journal_retention: Some(30 * 1000),
                    ..greeter_service_greet_handler()
                }],
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(30),
                journal_retention: Duration::from_secs(30),
            })
        )
    }

    #[test]
    fn service_level_journal_retention_with_idempotency_key() {
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                idempotency_retention: Some(120 * 1000),
                journal_retention: Some(60 * 1000),
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(true),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(120),
                journal_retention: Duration::from_secs(60),
            })
        )
    }

    #[test]
    fn handler_level_journal_retention_with_idempotency_key() {
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                idempotency_retention: Some(120 * 1000),
                journal_retention: Some(60 * 1000),
                handlers: vec![endpoint_manifest::Handler {
                    journal_retention: Some(30 * 1000),
                    ..greeter_service_greet_handler()
                }],
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(true),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(120),
                journal_retention: Duration::from_secs(30),
            })
        )
    }

    #[test]
    fn handler_level_journal_retention_overrides_the_service_level_journal_retention_with_idempotency_key()
     {
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(60 * 1000),
                handlers: vec![endpoint_manifest::Handler {
                    idempotency_retention: Some(120 * 1000),
                    journal_retention: Some(30 * 1000),
                    ..greeter_service_greet_handler()
                }],
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(true),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(120),
                journal_retention: Duration::from_secs(30),
            })
        )
    }

    #[test]
    fn journal_retention_greater_than_completion_retention() {
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(120 * 1000),
                handlers: vec![endpoint_manifest::Handler {
                    idempotency_retention: Some(60 * 1000),
                    ..greeter_service_greet_handler()
                }],
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(true),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(60),
                journal_retention: Duration::from_secs(60),
            })
        )
    }

    #[test]
    fn journal_retention_default_is_respected() {
        let mut config = Configuration::default();
        config.invocation.default_journal_retention = FriendlyDuration::from_secs(300);
        crate::config::set_current_config(config);

        let target = init_discover_and_resolve_target(
            greeter_service(),
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(300),
                journal_retention: Duration::from_secs(300),
            })
        )
    }

    #[test]
    fn journal_retention_default_is_overridden_by_discovery() {
        let mut config = Configuration::default();
        config.invocation.default_journal_retention = FriendlyDuration::from_secs(300);
        crate::config::set_current_config(config);

        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(60 * 1000),
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(60),
                journal_retention: Duration::from_secs(60),
            })
        )
    }

    #[test]
    fn journal_retention_last_one_wins_scenario() {
        // 1. User configures journal retention A in SDK configuration, registers the service, now journal retention is A
        crate::config::set_current_config(Configuration::default());

        let ((_, deployment_id), schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(add_deployment_request(vec![endpoint_manifest::Service {
                    journal_retention: Some(60 * 1000), // A = 60 seconds
                    ..greeter_service()
                }]))
            })
            .unwrap();

        schema.assert_service_revision(GREETER_SERVICE_NAME, 1);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id);

        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(60),
                journal_retention: Duration::from_secs(60),
            })
        );
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                journal_retention: some(eq(Duration::from_secs(60))),
            })
        );

        // 2. User updates journal retention to B in the UI, now journal retention is B
        let schema = SchemaUpdater::update(schema, |updater| {
            updater.modify_service(
                GREETER_SERVICE_NAME,
                vec![ModifyServiceChange::JournalRetention(Duration::from_secs(
                    120,
                ))], // B = 120 seconds
            )
        })
        .unwrap();

        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(120),
                journal_retention: Duration::from_secs(120),
            })
        );
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                journal_retention: some(eq(Duration::from_secs(120))),
            })
        );

        // 3. User registers a new revision of the service defining in the SDK journal retention A, now journal retention is again A
        let ((_, deployment_id), schema) =
            SchemaUpdater::update_and_return(schema, move |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    deployment_address: DeploymentAddress::mock_uri("http://localhost:9081"),
                    ..add_deployment_request(vec![endpoint_manifest::Service {
                        journal_retention: Some(60 * 1000), // A = 60 seconds again
                        ..greeter_service()
                    }])
                })
            })
            .unwrap();

        schema.assert_service_revision(GREETER_SERVICE_NAME, 2); // Revision should be incremented
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id);

        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(60),
                journal_retention: Duration::from_secs(60),
            })
        );
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                journal_retention: some(eq(Duration::from_secs(60))),
            })
        );

        // 4. Operator updates RESTATE_DEFAULT_JOURNAL_RETENTION with value C, journal retention is still A
        let mut config = Configuration::default();
        config.invocation.default_journal_retention = FriendlyDuration::from_secs(180); // C = 180 seconds
        crate::config::set_current_config(config);

        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(60),
                journal_retention: Duration::from_secs(60),
            })
        );
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                journal_retention: some(eq(Duration::from_secs(60))),
            })
        );

        // 5. Operator updates RESTATE_MAX_JOURNAL_RETENTION with value D, journal retention will be min(A, D)
        let mut config = Configuration::default();
        config.invocation.default_journal_retention = FriendlyDuration::from_secs(20); // E = 20 seconds -> this should be ignored
        config.invocation.max_journal_retention = Some(FriendlyDuration::from_secs(30)); // D = 30 seconds
        crate::config::set_current_config(config);

        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(30),
                journal_retention: Duration::from_secs(30),
            })
        );
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                journal_retention: some(eq(Duration::from_secs(30))),
            })
        );

        // 6. Operator registers a new version that doesn't declare journal retention, value should revert to the default E = 20 seconds
        let ((_, deployment_id), schema) =
            SchemaUpdater::update_and_return(schema, move |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    deployment_address: DeploymentAddress::mock_uri("http://localhost:9082"),
                    ..add_deployment_request(vec![endpoint_manifest::Service {
                        journal_retention: None,
                        ..greeter_service()
                    }])
                })
            })
            .unwrap();

        schema.assert_service_revision(GREETER_SERVICE_NAME, 3); // Revision should be incremented
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id);
        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(20),
                journal_retention: Duration::from_secs(20),
            })
        );
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                journal_retention: some(eq(Duration::from_secs(20))),
            })
        );
    }

    #[test]
    fn max_journal_retention_clamps_value_for_service() {
        // Set max_journal_retention to 60 seconds
        let mut config = Configuration::default();
        config.invocation.max_journal_retention = Some(FriendlyDuration::from_secs(60));
        crate::config::set_current_config(config);

        // Create a service with journal_retention of 120 seconds (higher than max)
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(120 * 1000), // 120 seconds
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );

        // Verify that the journal_retention is clamped to 60 seconds (the max)
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(60), // Clamped to max
                journal_retention: Duration::from_secs(60),    // Clamped to max
            })
        );
    }

    #[test]
    fn max_journal_retention_higher_than_set_value() {
        // Set max_journal_retention to 60 seconds
        let mut config = Configuration::default();
        config.invocation.max_journal_retention = Some(FriendlyDuration::from_secs(60));
        crate::config::set_current_config(config);

        // Create a service with journal_retention of 30 seconds (lower than max)
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(30 * 1000), // 30 seconds
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );

        // Verify that the journal_retention is not affected (still 30 seconds)
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(30), // Not clamped
                journal_retention: Duration::from_secs(30),    // Not clamped
            })
        );
    }

    #[test]
    fn max_journal_retention_clamps_value_for_handler() {
        // Set max_journal_retention to 60 seconds
        let mut config = Configuration::default();
        config.invocation.max_journal_retention = Some(FriendlyDuration::from_secs(60));
        crate::config::set_current_config(config);

        // Create a handler with journal_retention of 300 seconds (higher than max)
        let ((_, _), schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(add_deployment_request(vec![endpoint_manifest::Service {
                    journal_retention: Some(120 * 1000), // Service sets 120 seconds
                    handlers: vec![endpoint_manifest::Handler {
                        journal_retention: Some(300 * 1000), // 300 seconds
                        ..greeter_service_greet_handler()
                    }],
                    ..greeter_service()
                }]))
            })
            .unwrap();

        // Verify that the journal_retention is clamped to 60 seconds (the max)
        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(60), // Clamped to max
                journal_retention: Duration::from_secs(60),    // Clamped to max
            })
        );
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                journal_retention: some(eq(Duration::from_secs(60))), // This was never set
            })
        );
        assert_that!(
            schema.assert_handler(GREETER_SERVICE_NAME, GREET_HANDLER_NAME),
            pat!(HandlerMetadata {
                journal_retention: some(eq(Duration::from_secs(60))), // Clamped to max
            })
        );

        // Disable max
        let mut config = Configuration::default();
        config.invocation.max_journal_retention = None;
        crate::config::set_current_config(config);

        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(300),
                journal_retention: Duration::from_secs(300),
            })
        );
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                journal_retention: some(eq(Duration::from_secs(120))), // The initial value set
            })
        );
        assert_that!(
            schema.assert_handler(GREETER_SERVICE_NAME, GREET_HANDLER_NAME),
            pat!(HandlerMetadata {
                journal_retention: some(eq(Duration::from_secs(300))), // The initial value set
            })
        );
    }

    #[test]
    fn max_journal_retention_unset_means_no_limit() {
        // Set default_journal_retention but leave max_journal_retention unset
        let mut config = Configuration::default();
        config.invocation.default_journal_retention = FriendlyDuration::from_secs(60);
        config.invocation.max_journal_retention = None; // Explicitly unset
        crate::config::set_current_config(config);

        // Create a service with a very high journal_retention
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(3600 * 1000), // 1 hour
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );

        // Verify that the journal_retention is not clamped (no limit)
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(3600), // Not clamped
                journal_retention: Duration::from_secs(3600),    // Not clamped
            })
        );
    }

    #[test]
    fn max_journal_retention_zero_disables_journal_retention() {
        // Set max_journal_retention to 0 (always disabled)
        let mut config = Configuration::default();
        config.invocation.max_journal_retention = Some(FriendlyDuration::from_secs(0));
        crate::config::set_current_config(config);

        // Create a service with journal_retention set
        let target = init_discover_and_resolve_target(
            endpoint_manifest::Service {
                journal_retention: Some(60 * 1000), // 60 seconds
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );

        // Verify that the journal_retention is clamped to 0 (disabled)
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(0), // Clamped to 0
                journal_retention: Duration::from_secs(0),    // Clamped to 0
            })
        );
    }

    #[test]
    fn max_journal_retention_zero_wins_over_default_and_set_values() {
        // Create a service with default journal_retention
        let mut config = Configuration::default();
        config.invocation.default_journal_retention = FriendlyDuration::from_secs(300);
        config.invocation.max_journal_retention = Some(FriendlyDuration::from_secs(0));
        crate::config::set_current_config(config);

        let target = init_discover_and_resolve_target(
            greeter_service(), // No explicit journal_retention
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );

        // Verify that the journal_retention is still 0 (disabled)
        assert_that!(
            target.compute_retention(false),
            eq(InvocationRetention {
                completion_retention: Duration::from_secs(0), // Clamped to 0
                journal_retention: Duration::from_secs(0),    // Clamped to 0
            })
        );
    }

    fn init_discover_and_resolve_timeouts(
        svc: endpoint_manifest::Service,
        service_name: &str,
        handler_name: &str,
    ) -> InvocationAttemptOptions {
        let schema_information = Schema::default();
        let mut updater = SchemaUpdater::new(schema_information);

        let deployment_id = updater
            .add_deployment(add_deployment_request(vec![svc]))
            .unwrap()
            .1;

        let schema = updater.into_inner();

        schema.assert_service_revision(service_name, 1);
        schema.assert_service_deployment(service_name, deployment_id);

        schema
            .resolve_invocation_attempt_options(&deployment_id, service_name, handler_name)
            .unwrap_or_else(|| {
                panic!(
                    "Invocation target for deployment {} and target {}/{} must exists",
                    deployment_id, service_name, handler_name
                )
            })
    }

    #[test]
    fn service_level_timeouts() {
        let timeouts = init_discover_and_resolve_timeouts(
            endpoint_manifest::Service {
                abort_timeout: Some(120 * 1000),
                inactivity_timeout: Some(60 * 1000),
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            timeouts,
            eq(InvocationAttemptOptions {
                abort_timeout: Some(Duration::from_secs(120)),
                inactivity_timeout: Some(Duration::from_secs(60)),
                enable_lazy_state: None,
            })
        )
    }

    #[test]
    fn service_level_timeouts_with_handler_overrides() {
        let timeouts = init_discover_and_resolve_timeouts(
            endpoint_manifest::Service {
                abort_timeout: Some(120 * 1000),
                inactivity_timeout: Some(60 * 1000),
                handlers: vec![endpoint_manifest::Handler {
                    inactivity_timeout: Some(30 * 1000),
                    ..greeter_service_greet_handler()
                }],
                ..greeter_service()
            },
            GREETER_SERVICE_NAME,
            GREET_HANDLER_NAME,
        );
        assert_that!(
            timeouts,
            eq(InvocationAttemptOptions {
                abort_timeout: Some(Duration::from_secs(120)),
                inactivity_timeout: Some(Duration::from_secs(30)),
                enable_lazy_state: None,
            })
        )
    }
}

mod modify_service {
    use super::*;

    use crate::config::{Configuration, DEFAULT_ABORT_TIMEOUT, DEFAULT_INACTIVITY_TIMEOUT};
    use crate::invocation::InvocationRetention;
    use crate::schema::invocation_target::{InvocationAttemptOptions, InvocationTargetMetadata};
    use crate::schema::service::ServiceMetadata;
    use googletest::prelude::*;
    use restate_time_util::FriendlyDuration;
    use test_log::test;

    #[test]
    fn workflow_retention() {
        // Register a plain workflow first
        let ((_, deployment_id), mut schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(add_deployment_request(vec![greeter_workflow()]))
            })
            .unwrap();

        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                revision: eq(1),
                deployment_id: eq(deployment_id),
                workflow_completion_retention: eq(Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION))
            })
        );
        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: DEFAULT_WORKFLOW_COMPLETION_RETENTION,
                journal_retention: DEFAULT_WORKFLOW_COMPLETION_RETENTION,
            })
        );

        // Now update it
        let new_retention = Duration::from_secs(30);
        schema = SchemaUpdater::update(schema, |updater| {
            updater.modify_service(
                GREETER_SERVICE_NAME,
                vec![ModifyServiceChange::WorkflowCompletionRetention(
                    new_retention,
                )],
            )
        })
        .unwrap();

        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                workflow_completion_retention: eq(Some(new_retention))
            })
        );
        assert_that!(
            schema
                .assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME)
                .compute_retention(false),
            eq(InvocationRetention {
                completion_retention: new_retention,
                journal_retention: new_retention,
            })
        );
    }

    #[test]
    fn modify_all_service_config_options_then_register_second_version() {
        const DEFAULT_JOURNAL_RETENTION: Duration = Duration::from_secs(60 * 60 * 24);

        let mut config = Configuration::default();
        config.invocation.default_journal_retention =
            FriendlyDuration::new(DEFAULT_JOURNAL_RETENTION);
        crate::config::set_current_config(config);

        // Register a plain service first
        let ((_, deployment_id), mut schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(add_deployment_request(vec![greeter_service()]))
            })
            .unwrap();

        // Verify initial state
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                revision: eq(1),
                deployment_id: eq(deployment_id),
                public: eq(true), // default is public
                idempotency_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                journal_retention: eq(Some(DEFAULT_JOURNAL_RETENTION)),
                inactivity_timeout: eq(DEFAULT_INACTIVITY_TIMEOUT),
                abort_timeout: eq(DEFAULT_ABORT_TIMEOUT),
            })
        );

        // Update using modify_service (e.g. through UI/CLI) the service options
        let new_public = false;
        let new_idempotency_retention = Duration::from_secs(120);
        let new_journal_retention = Duration::from_secs(300);
        let new_inactivity_timeout = Duration::from_secs(30);
        let new_abort_timeout = Duration::from_secs(60);
        schema = SchemaUpdater::update(schema, |updater| {
            updater.modify_service(
                GREETER_SERVICE_NAME,
                vec![
                    ModifyServiceChange::Public(new_public),
                    ModifyServiceChange::IdempotencyRetention(new_idempotency_retention),
                    ModifyServiceChange::JournalRetention(new_journal_retention),
                    ModifyServiceChange::InactivityTimeout(new_inactivity_timeout),
                    ModifyServiceChange::AbortTimeout(new_abort_timeout),
                ],
            )
        })
        .unwrap();

        // Verify all changes have been applied
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                public: eq(new_public),
                idempotency_retention: eq(new_idempotency_retention),
                journal_retention: eq(Some(new_journal_retention)),
                inactivity_timeout: eq(new_inactivity_timeout),
                abort_timeout: eq(new_abort_timeout),
            })
        );

        // Now register a second version of greeter, identical to the first
        let ((_, deployment_id_2), schema_after_second_deployment) =
            SchemaUpdater::update_and_return(schema, move |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    deployment_address: DeploymentAddress::mock_uri("http://localhost:9082"),
                    ..add_deployment_request(vec![greeter_service()]) // identical service definition
                })
            })
            .unwrap();

        // Verify that the config option changes have been blanked/reset to defaults
        assert_that!(
            schema_after_second_deployment.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                revision: eq(2),                    // revision should be incremented
                deployment_id: eq(deployment_id_2), // new deployment

                // -- Back to default
                public: eq(true),
                idempotency_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                journal_retention: eq(Some(DEFAULT_JOURNAL_RETENTION)),
                inactivity_timeout: eq(DEFAULT_INACTIVITY_TIMEOUT),
                abort_timeout: eq(DEFAULT_ABORT_TIMEOUT),
            })
        );
    }

    #[test]
    fn modify_all_workflow_config_options_then_register_second_version() {
        const DEFAULT_JOURNAL_RETENTION: Duration = Duration::from_secs(60 * 60 * 24);

        let mut config = Configuration::default();
        config.invocation.default_journal_retention =
            FriendlyDuration::new(DEFAULT_JOURNAL_RETENTION);
        crate::config::set_current_config(config);

        // Register a plain service first
        let ((_, deployment_id), mut schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(add_deployment_request(vec![greeter_workflow()]))
            })
            .unwrap();

        // Verify initial state
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                revision: eq(1),
                deployment_id: eq(deployment_id),
                public: eq(true), // default is public
                idempotency_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                journal_retention: eq(Some(DEFAULT_JOURNAL_RETENTION)),
                workflow_completion_retention: eq(Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)),
                inactivity_timeout: eq(DEFAULT_INACTIVITY_TIMEOUT),
                abort_timeout: eq(DEFAULT_ABORT_TIMEOUT),
            })
        );

        // Update using modify_service (e.g. through UI/CLI) the service options
        let new_public = false;
        let new_idempotency_retention = Duration::from_secs(120);
        let new_workflow_completion_retention = Duration::from_secs(110);
        let new_journal_retention = Duration::from_secs(300);
        let new_inactivity_timeout = Duration::from_secs(30);
        let new_abort_timeout = Duration::from_secs(60);
        schema = SchemaUpdater::update(schema, |updater| {
            updater.modify_service(
                GREETER_SERVICE_NAME,
                vec![
                    ModifyServiceChange::Public(new_public),
                    ModifyServiceChange::IdempotencyRetention(new_idempotency_retention),
                    ModifyServiceChange::JournalRetention(new_journal_retention),
                    ModifyServiceChange::WorkflowCompletionRetention(
                        new_workflow_completion_retention,
                    ),
                    ModifyServiceChange::InactivityTimeout(new_inactivity_timeout),
                    ModifyServiceChange::AbortTimeout(new_abort_timeout),
                ],
            )
        })
        .unwrap();

        // Verify all changes have been applied
        assert_that!(
            schema.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                public: eq(new_public),
                idempotency_retention: eq(new_idempotency_retention),
                journal_retention: eq(Some(new_journal_retention)),
                workflow_completion_retention: eq(Some(new_workflow_completion_retention)),
                inactivity_timeout: eq(new_inactivity_timeout),
                abort_timeout: eq(new_abort_timeout),
            })
        );
        assert_that!(
            schema.assert_invocation_target(GREETER_SERVICE_NAME, GREET_HANDLER_NAME),
            pat!(InvocationTargetMetadata {
                completion_retention: eq(new_workflow_completion_retention),
                journal_retention: eq(new_journal_retention),
            })
        );
        assert_that!(
            schema.resolve_invocation_attempt_options(
                &deployment_id,
                GREETER_SERVICE_NAME,
                GREET_HANDLER_NAME
            ),
            some(pat!(InvocationAttemptOptions {
                inactivity_timeout: some(eq(new_inactivity_timeout)),
                abort_timeout: some(eq(new_abort_timeout)),
            }))
        );

        // Now register a second version of greeter, identical to the first
        let ((_, deployment_id_2), schema_after_second_deployment) =
            SchemaUpdater::update_and_return(schema, move |updater| {
                updater.add_deployment(AddDeploymentRequest {
                    deployment_address: DeploymentAddress::mock_uri("http://localhost:9082"),
                    ..add_deployment_request(vec![greeter_workflow()]) // identical service definition
                })
            })
            .unwrap();

        // Verify that the config option changes have been blanked/reset to defaults
        assert_that!(
            schema_after_second_deployment.assert_service(GREETER_SERVICE_NAME),
            pat!(ServiceMetadata {
                revision: eq(2),                    // revision should be incremented
                deployment_id: eq(deployment_id_2), // new deployment

                // -- Back to default
                public: eq(true),
                idempotency_retention: eq(DEFAULT_IDEMPOTENCY_RETENTION),
                journal_retention: eq(Some(DEFAULT_JOURNAL_RETENTION)),
                workflow_completion_retention: eq(Some(DEFAULT_WORKFLOW_COMPLETION_RETENTION)),
                inactivity_timeout: eq(DEFAULT_INACTIVITY_TIMEOUT),
                abort_timeout: eq(DEFAULT_ABORT_TIMEOUT),
            })
        );
    }
}
