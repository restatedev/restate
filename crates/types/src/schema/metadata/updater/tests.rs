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

use http::HeaderName;
use restate_test_util::{assert, assert_eq};
use test_log::test;

use crate::Versioned;
use crate::schema::deployment::Deployment;
use crate::schema::deployment::DeploymentResolver;
use crate::schema::invocation_target::InvocationTargetResolver;
use crate::schema::service::ServiceMetadataResolver;

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

#[test]
fn register_new_deployment() {
    let schema_information = Schema::default();
    let initial_version = schema_information.version();
    let mut updater = SchemaUpdater::new(schema_information);

    let mut deployment = Deployment::mock();
    deployment.id = updater
        .add_deployment(
            deployment.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

    let schema = updater.into_inner();

    assert!(initial_version < schema.version());
    schema.assert_service_revision(GREETER_SERVICE_NAME, 1);
    schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
    schema.assert_invocation_target(GREETER_SERVICE_NAME, "greet");
}

#[test]
fn register_new_deployment_add_unregistered_service() {
    let mut updater = SchemaUpdater::default();

    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
    let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

    // Register first deployment
    deployment_1.id = updater
        .add_deployment(
            deployment_1.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
    assert!(
        schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none()
    );

    updater = SchemaUpdater::new(schemas);
    deployment_2.id = updater
        .add_deployment(
            deployment_2.metadata.clone(),
            None,
            vec![greeter_service(), another_greeter_service()],
            false,
        )
        .unwrap();
    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_2.id);
    schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
}

#[test]
fn register_new_deployment_discriminate_on_routing_header() {
    let deployment = Deployment::mock_with_uri("http://localhost:9080");

    // Register first deployment
    let (deployment_id_1, schema) =
        SchemaUpdater::update_and_return(Schema::default(), |updater| {
            updater.add_deployment(
                deployment.metadata.clone(),
                Some((
                    HeaderName::from_static("x-routing"),
                    HeaderValue::from_static("1"),
                )),
                vec![greeter_service()],
                false,
            )
        })
        .unwrap();
    schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_1);

    // Update providing another routing header
    let (deployment_id_2, schema) = SchemaUpdater::update_and_return(schema, |updater| {
        updater.add_deployment(
            deployment.metadata.clone(),
            Some((
                HeaderName::from_static("x-routing"),
                HeaderValue::from_static("2"),
            )),
            vec![greeter_service()],
            false,
        )
    })
    .unwrap();
    schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment_id_2);

    // Update providing no routing header -> conflict
    let update_result = SchemaUpdater::update_and_return(schema.clone(), |updater| {
        updater.add_deployment(
            deployment.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
    });
    assert!(let SchemaError::Override(_) = update_result.unwrap_err());

    // Update providing the same routing header-> conflict
    let update_result = SchemaUpdater::update_and_return(schema, |updater| {
        updater.add_deployment(
            deployment.metadata.clone(),
            Some((
                HeaderName::from_static("x-routing"),
                HeaderValue::from_static("2"),
            )),
            vec![greeter_service()],
            false,
        )
    });
    assert!(let SchemaError::Override(_) = update_result.unwrap_err());
}

/// This test case ensures that https://github.com/restatedev/restate/issues/1205 works
#[test]
fn force_deploy_private_service() -> Result<(), SchemaError> {
    let mut updater = SchemaUpdater::default();
    let mut deployment = Deployment::mock();

    deployment.id = updater.add_deployment(
        deployment.metadata.clone(),
        None,
        vec![greeter_service()],
        false,
    )?;

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
    deployment.id = updater.add_deployment(
        deployment.metadata.clone(),
        None,
        vec![greeter_service()],
        true,
    )?;

    let schemas = updater.into_inner();
    assert!(schemas.assert_service(GREETER_SERVICE_NAME).public);
    assert!(
        schemas
            .assert_invocation_target(GREETER_SERVICE_NAME, "greet")
            .public
    );

    Ok(())
}

mod change_service_type {
    use super::*;

    use restate_test_util::{assert, assert_eq};
    use test_log::test;

    #[test]
    fn fails_without_force() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                None,
                vec![greeter_service()],
                false,
            )
            .unwrap();
        let schemas = updater.into_inner();

        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);

        let compute_result = SchemaUpdater::new(schemas).add_deployment(
            deployment_2.metadata,
            None,
            vec![greeter_virtual_object()],
            false,
        );

        assert!(let &SchemaError::Service(
                ServiceError::DifferentType(_)
            ) = compute_result.unwrap_err());
    }

    #[test]
    fn works_with_force() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                None,
                vec![greeter_service()],
                false,
            )
            .unwrap();
        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);

        updater = SchemaUpdater::new(schemas);
        deployment_2.id = updater
            .add_deployment(
                deployment_2.metadata,
                None,
                vec![greeter_virtual_object()],
                true,
            )
            .unwrap();
        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);

        assert_eq!(
            schemas.assert_service(GREETER_SERVICE_NAME).ty,
            ServiceType::VirtualObject
        );
    }

    #[test]
    fn works_with_force_and_correctly_set_workflow_retention() {
        let mut updater = SchemaUpdater::default();

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata.clone(),
                None,
                vec![greeter_service()],
                false,
            )
            .unwrap();
        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);

        updater = SchemaUpdater::new(schemas);
        deployment_2.id = updater
            .add_deployment(deployment_2.metadata, None, vec![greeter_workflow()], true)
            .unwrap();
        let schemas = updater.into_inner();
        schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);

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
    let mut deployment = Deployment::mock();
    let (deployment_id, schemas) = SchemaUpdater::update_and_return(Schema::default(), |updater| {
        updater.add_deployment(
            deployment.metadata.clone(),
            None,
            vec![greeter_service(), another_greeter_service()],
            false,
        )
    })
    .unwrap();
    deployment.id = deployment_id;

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment.id);

    let (deployment_id, schemas) = SchemaUpdater::update_and_return(schemas, |updater| {
        updater.add_deployment(
            deployment.metadata.clone(),
            None,
            vec![greeter_service()],
            true,
        )
    })
    .unwrap();
    assert_eq!(deployment_id, deployment.id);

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
    assert!(
        schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none()
    );
}

#[test]
fn cannot_override_existing_deployment_endpoint_conflict() {
    let mut updater = SchemaUpdater::default();

    let mut deployment = Deployment::mock();
    deployment.id = updater
        .add_deployment(
            deployment.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

    assert!(let SchemaError::Override(_) = updater.add_deployment(
        deployment.metadata,
        None,
        vec![greeter_service()],
        false).unwrap_err()
    );
}

#[test]
fn register_two_deployments_then_remove_first() {
    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
    let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

    let (deployment_id_1, schemas) =
        SchemaUpdater::update_and_return(Schema::default(), |updater| {
            updater.add_deployment(
                deployment_1.metadata.clone(),
                None,
                vec![greeter_service(), another_greeter_service()],
                false,
            )
        })
        .unwrap();
    deployment_1.id = deployment_id_1;

    let (deployment_id_2, schemas) = SchemaUpdater::update_and_return(schemas, |updater| {
        updater.add_deployment(
            deployment_2.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
    })
    .unwrap();
    deployment_2.id = deployment_id_2;

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
    schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);

    let version_before_removal = schemas.version();
    let schemas = SchemaUpdater::update(schemas, |updater| {
        let _: () = updater.remove_deployment(deployment_1.id);
        Ok::<(), Infallible>(())
    })
    .unwrap();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    assert!(version_before_removal < schemas.version());
    assert!(
        schemas
            .resolve_latest_service(ANOTHER_GREETER_SERVICE_NAME)
            .is_none()
    );
    assert!(schemas.get_deployment(&deployment_1.id).is_none());
}

#[test]
fn register_two_deployments_then_remove_second() {
    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
    let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

    let (deployment_id_1, schemas) =
        SchemaUpdater::update_and_return(Schema::default(), |updater| {
            updater.add_deployment(
                deployment_1.metadata.clone(),
                None,
                vec![greeter_service(), another_greeter_service()],
                false,
            )
        })
        .unwrap();
    deployment_1.id = deployment_id_1;

    let (deployment_id_2, schemas) = SchemaUpdater::update_and_return(schemas, |updater| {
        updater.add_deployment(
            deployment_2.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
    })
    .unwrap();
    deployment_2.id = deployment_id_2;

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
    schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);

    let version_before_removal = schemas.version();
    let schemas = SchemaUpdater::update(schemas, |updater| {
        let _: () = updater.remove_deployment(deployment_2.id);
        Ok::<(), Infallible>(())
    })
    .unwrap();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);
    assert!(version_before_removal < schemas.version());
    schemas.assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
    schemas.assert_service_revision(ANOTHER_GREETER_SERVICE_NAME, 1);
    assert!(schemas.get_deployment(&deployment_2.id).is_none());
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

        let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

        deployment_1.id = updater
            .add_deployment(
                deployment_1.metadata,
                None,
                vec![greeter_v1_service()],
                false,
            )
            .unwrap();
        let schemas = updater.into_inner();
        schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);

        updater = SchemaUpdater::new(schemas);
        let rejection = updater
            .add_deployment(
                deployment_2.metadata,
                None,
                vec![greeter_v2_service()],
                false,
            )
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

    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");

    deployment_1.id = updater
        .add_deployment(
            deployment_1.metadata.clone(),
            None,
            vec![greeter_virtual_object()],
            false,
        )
        .unwrap();

    assert!(let &SchemaError::NotFound(_) = updater.update_deployment(
            DeploymentId::new(),
            deployment_1.metadata.clone(),
            vec![],
        ).unwrap_err());

    assert!(let &SchemaError::Deployment(
            DeploymentError::RemovedServices(_)
        ) = updater.update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![],
        ).unwrap_err());

    {
        let mut greeter_virtual_object = greeter_virtual_object();
        greeter_virtual_object.ty = endpoint_manifest::ServiceType::Service;

        assert!(let &SchemaError::Service(
                ServiceError::DifferentType(_)
            ) = updater.update_deployment(
                deployment_1.id,
                deployment_1.metadata.clone(),
                vec![greeter_virtual_object],
            ).unwrap_err());
    }

    assert!(let &SchemaError::Service(
            ServiceError::RemovedHandlers(_, _)
        ) = updater.update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![endpoint_manifest::Service {
                handlers: Default::default(),
                ..greeter_virtual_object()
            }],
        ).unwrap_err());

    deployment_1
        .metadata
        .delivery_options
        .additional_headers
        .insert(
            HeaderName::from_static("foo"),
            HeaderValue::from_static("bar"),
        );

    updater
        .update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![greeter_virtual_object()],
        )
        .unwrap();

    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 1);

    let updated_deployment = schemas.get_deployment(&deployment_1.id).unwrap();

    assert!(
        updated_deployment
            .metadata
            .delivery_options
            .additional_headers
            .contains_key(&HeaderName::from_static("foo"))
    );
}

#[test]
fn update_draining_deployment() {
    let mut updater = SchemaUpdater::default();

    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
    let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

    deployment_1.id = updater
        .add_deployment(
            deployment_1.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

    deployment_2.id = updater
        .add_deployment(
            deployment_2.metadata.clone(),
            None,
            vec![greeter_service(), another_greeter_service()],
            false,
        )
        .unwrap();

    deployment_1
        .metadata
        .delivery_options
        .additional_headers
        .insert(
            HeaderName::from_static("foo"),
            HeaderValue::from_static("bar"),
        );

    updater
        .update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![greeter_service()],
        )
        .unwrap();

    let schemas = updater.into_inner();

    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);

    let updated_deployment = schemas.get_deployment(&deployment_1.id).unwrap();

    assert!(
        updated_deployment
            .metadata
            .delivery_options
            .additional_headers
            .contains_key(&HeaderName::from_static("foo"))
    );
}

#[test]
fn update_deployment_same_uri() {
    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
    let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

    let (deployment_id_1, schemas) =
        SchemaUpdater::update_and_return(Schema::default(), |updater| {
            updater.add_deployment(
                deployment_1.metadata.clone(),
                None,
                vec![greeter_service()],
                false,
            )
        })
        .unwrap();
    deployment_1.id = deployment_id_1;

    // patching new invocations
    let (deployment_id_2, schemas) = SchemaUpdater::update_and_return(schemas, |updater| {
        updater.add_deployment(
            deployment_2.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
    })
    .unwrap();
    deployment_2.id = deployment_id_2;

    // oh, I have some old failing invocations, wish those were on the patched version too

    deployment_1.metadata.ty = deployment_2.metadata.ty.clone();
    let schemas = SchemaUpdater::update(schemas, |updater| {
        updater.update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![greeter_service()],
        )
    })
    .unwrap();

    // there are now two deployment IDs pointing to :9081, so we shouldn't be able to force either of them
    assert!(let &SchemaError::Deployment(
            DeploymentError::MultipleExistingDeployments(_)
        ) = SchemaUpdater::update(schemas.clone(), |updater| updater.add_deployment(
            deployment_1.metadata.clone(),None,
            vec![greeter_service(), greeter_virtual_object()],
            true,
        ).map(|_| ())).unwrap_err());

    assert!(let &SchemaError::Deployment(
            DeploymentError::MultipleExistingDeployments(_)
        ) = SchemaUpdater::update(schemas.clone(), |updater| updater.add_deployment(
            deployment_2.metadata.clone(),None,
            vec![greeter_service(), greeter_virtual_object()],
            true,
   ).map(|_| ())).unwrap_err());

    // Latest should remain deployment_2
    schemas.assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
    schemas.assert_service_revision(GREETER_SERVICE_NAME, 2);

    let updated_deployment_1 = schemas.get_deployment(&deployment_1.id).unwrap();
    let updated_deployment_2 = schemas.get_deployment(&deployment_2.id).unwrap();

    assert_eq!(
        updated_deployment_1.metadata.ty,
        updated_deployment_2.metadata.ty
    );

    // the failing invocations have drained so I can safely delete the original deployment
    let schemas = SchemaUpdater::update(schemas, |updater| {
        let _: () = updater.remove_deployment(deployment_1.id);
        Ok::<(), Infallible>(())
    })
    .unwrap();

    assert_eq!(
        deployment_2.id,
        SchemaUpdater::update_and_return(schemas, |updater| updater.add_deployment(
            deployment_2.metadata.clone(),
            None,
            vec![greeter_service(), greeter_virtual_object()],
            true,
        ))
        .unwrap()
        .0
    );
}

#[test]
fn update_latest_deployment_add_handler() {
    let mut updater = SchemaUpdater::default();

    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9081");

    deployment_1.id = updater
        .add_deployment(
            deployment_1.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

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
        .update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![updated_greeter_service],
        )
        .unwrap();

    updater
        .schema
        .assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
    updater
        .schema
        .assert_service_revision(GREETER_SERVICE_NAME, 1);

    let (_, services) = updater
        .schema
        .get_deployment_and_services(&deployment_1.id)
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

    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
    let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

    deployment_1.id = updater
        .add_deployment(
            deployment_1.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

    deployment_2.id = updater
        .add_deployment(
            deployment_2.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

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
        .update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![updated_greeter_service],
        )
        .unwrap();

    updater
        .schema
        .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
    updater
        .schema
        .assert_service_revision(GREETER_SERVICE_NAME, 2);

    let (_, updated_deployment_1_services) = updater
        .schema
        .get_deployment_and_services(&deployment_1.id)
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
        .get_deployment_and_services(&deployment_2.id)
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

    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");

    deployment_1.id = updater
        .add_deployment(
            deployment_1.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

    updater
        .update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![greeter_service(), another_greeter_service()],
        )
        .unwrap();

    updater
        .schema
        .assert_service_deployment(GREETER_SERVICE_NAME, deployment_1.id);
    updater
        .schema
        .assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
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

    let mut deployment_1 = Deployment::mock_with_uri("http://localhost:9080");
    let mut deployment_2 = Deployment::mock_with_uri("http://localhost:9081");

    deployment_1.id = updater
        .add_deployment(
            deployment_1.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

    deployment_2.id = updater
        .add_deployment(
            deployment_2.metadata.clone(),
            None,
            vec![greeter_service()],
            false,
        )
        .unwrap();

    updater
        .update_deployment(
            deployment_1.id,
            deployment_1.metadata.clone(),
            vec![greeter_service(), another_greeter_service()],
        )
        .unwrap();

    updater
        .schema
        .assert_service_deployment(GREETER_SERVICE_NAME, deployment_2.id);
    updater
        .schema
        .assert_service_deployment(ANOTHER_GREETER_SERVICE_NAME, deployment_1.id);
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
    let mut deployment = Deployment::mock();

    deployment.id = updater.add_deployment(
        deployment.metadata.clone(),
        None,
        vec![greeter_service()],
        false,
    )?;

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
    updater.update_deployment(
        deployment.id,
        deployment.metadata.clone(),
        vec![greeter_service()],
    )?;

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
    use crate::schema::deployment::Deployment;
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
        let mut deployment = Deployment::mock();
        let (deployment_id, schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(deployment.metadata.clone(), None, vec![svc], false)
            })
            .unwrap();
        deployment.id = deployment_id;

        schema.assert_service_revision(service_name, 1);
        schema.assert_service_deployment(service_name, deployment.id);

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
                .add_deployment(
                    Deployment::mock().metadata,
                    None,
                    vec![endpoint_manifest::Service {
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
                    }],
                    false,
                )
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
        let deployment = Deployment::mock();

        assert_that!(
            updater.add_deployment(
                deployment.metadata.clone(),
                None,
                vec![endpoint_manifest::Service {
                    ingress_private: Some(true),
                    handlers: vec![endpoint_manifest::Handler {
                        ingress_private: Some(false),
                        name: "my_private_handler".parse().unwrap(),
                        ..greeter_service_greet_handler()
                    }],
                    ..greeter_service()
                }],
                false
            ),
            err(pat!(SchemaError::Service(pat!(
                ServiceError::BadHandlerVisibility { .. }
            ))))
        );
    }

    #[test]
    fn workflow_retention() {
        let (deployment_id, schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(
                    Deployment::mock().metadata.clone(),
                    None,
                    vec![endpoint_manifest::Service {
                        handlers: vec![endpoint_manifest::Handler {
                            workflow_completion_retention: Some(30 * 1000),
                            ..greeter_workflow_greet_handler()
                        }],
                        ..greeter_workflow()
                    }],
                    false,
                )
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

        let mut deployment = Deployment::mock();
        let (deployment_id, schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(
                    deployment.metadata.clone(),
                    None,
                    vec![endpoint_manifest::Service {
                        journal_retention: Some(60 * 1000), // A = 60 seconds
                        ..greeter_service()
                    }],
                    false,
                )
            })
            .unwrap();
        deployment.id = deployment_id;

        schema.assert_service_revision(GREETER_SERVICE_NAME, 1);
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);

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
        let mut deployment = Deployment::mock_with_uri("http://localhost:9081");
        let (deployment_id, schema) = SchemaUpdater::update_and_return(schema, move |updater| {
            updater.add_deployment(
                deployment.metadata.clone(),
                None,
                vec![endpoint_manifest::Service {
                    journal_retention: Some(60 * 1000), // A = 60 seconds again
                    ..greeter_service()
                }],
                false,
            )
        })
        .unwrap();
        deployment.id = deployment_id;

        schema.assert_service_revision(GREETER_SERVICE_NAME, 2); // Revision should be incremented
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);

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
        let mut deployment = Deployment::mock_with_uri("http://localhost:9082");
        let (deployment_id, schema) = SchemaUpdater::update_and_return(schema, move |updater| {
            updater.add_deployment(
                deployment.metadata.clone(),
                None,
                vec![endpoint_manifest::Service {
                    journal_retention: None,
                    ..greeter_service()
                }],
                false,
            )
        })
        .unwrap();
        deployment.id = deployment_id;

        schema.assert_service_revision(GREETER_SERVICE_NAME, 3); // Revision should be incremented
        schema.assert_service_deployment(GREETER_SERVICE_NAME, deployment.id);
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
        let mut deployment = Deployment::mock();
        let (deployment_id, schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(
                    deployment.metadata.clone(),
                    None,
                    vec![endpoint_manifest::Service {
                        journal_retention: Some(120 * 1000), // Service sets 120 seconds
                        handlers: vec![endpoint_manifest::Handler {
                            journal_retention: Some(300 * 1000), // 300 seconds
                            ..greeter_service_greet_handler()
                        }],
                        ..greeter_service()
                    }],
                    false,
                )
            })
            .unwrap();
        deployment.id = deployment_id;

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

        let mut deployment = Deployment::mock();
        deployment.id = updater
            .add_deployment(deployment.metadata.clone(), None, vec![svc], false)
            .unwrap();

        let schema = updater.into_inner();

        schema.assert_service_revision(service_name, 1);
        schema.assert_service_deployment(service_name, deployment.id);

        schema
            .resolve_invocation_attempt_options(&deployment.id, service_name, handler_name)
            .unwrap_or_else(|| {
                panic!(
                    "Invocation target for deployment {} and target {}/{} must exists",
                    deployment.id, service_name, handler_name
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
        let (deployment_id, mut schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(
                    Deployment::mock().metadata.clone(),
                    None,
                    vec![greeter_workflow()],
                    false,
                )
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
        let (deployment_id, mut schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(
                    Deployment::mock().metadata.clone(),
                    None,
                    vec![greeter_service()],
                    false,
                )
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
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9082");
        let (deployment_id_2, schema_after_second_deployment) =
            SchemaUpdater::update_and_return(schema, move |updater| {
                updater.add_deployment(
                    deployment_2.metadata.clone(),
                    None,
                    vec![greeter_service()], // identical service definition
                    false,
                )
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
        let (deployment_id, mut schema) =
            SchemaUpdater::update_and_return(Schema::default(), move |updater| {
                updater.add_deployment(
                    Deployment::mock().metadata.clone(),
                    None,
                    vec![greeter_workflow()],
                    false,
                )
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
        let deployment_2 = Deployment::mock_with_uri("http://localhost:9082");
        let (deployment_id_2, schema_after_second_deployment) =
            SchemaUpdater::update_and_return(schema, move |updater| {
                updater.add_deployment(
                    deployment_2.metadata.clone(),
                    None,
                    vec![greeter_workflow()], // identical service definition
                    false,
                )
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
