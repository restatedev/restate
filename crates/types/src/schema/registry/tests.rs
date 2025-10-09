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

use crate::endpoint_manifest;
use crate::schema::registry::mocks::mock_arc_schema;
use restate_test_util::assert_eq;
use test_log::test;

const GREETER_SERVICE_NAME: &str = "greeter.Greeter";
const GREET_HANDLER_NAME: &str = "greet";

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

#[test(tokio::test)]
pub async fn register_deployment_lambda() {
    let schema_metadata = mock_arc_schema();
    let schema_registry = SchemaRegistry::new(
        schema_metadata.clone(),
        DiscoveryResponse {
            deployment_type_parameters: DeploymentConnectionParameters::Lambda {
                compression: None,
            },
            ..DiscoveryResponse::mock(vec![greeter_service()])
        },
        (),
    );

    let arn_1: LambdaARN = "arn:aws:lambda:region:account:function:lambda-function:1"
        .parse()
        .unwrap();
    let register_deployment_request_1 = RegisterDeploymentRequest {
        deployment_address: DeploymentAddress::Lambda(LambdaDeploymentAddress::new(
            arn_1.clone(),
            None,
        )),
        additional_headers: Default::default(),
        metadata: Default::default(),
        use_http_11: false,
        allow_breaking: AllowBreakingChanges::No,
        overwrite: Overwrite::No,
        apply_mode: ApplyMode::Apply,
    };

    // Let's register first time.
    let (add_deployment_result, deployment_1, _) = schema_registry
        .register_deployment(register_deployment_request_1.clone())
        .await
        .unwrap();
    assert_eq!(add_deployment_result, AddDeploymentResult::Created);
    assert_eq!(
        deployment_1.ty.as_address(),
        LambdaDeploymentAddress::new(arn_1.clone(), None).into()
    );
    schema_metadata
        .get()
        .assert_service_revision(GREETER_SERVICE_NAME, 1);

    // Let's register the second time, should be unchanged and id should be the same
    let (add_deployment_result, expected_deployment_1, _) = schema_registry
        .register_deployment(register_deployment_request_1)
        .await
        .unwrap();
    assert_eq!(add_deployment_result, AddDeploymentResult::Unchanged);
    assert_eq!(deployment_1.id, expected_deployment_1.id);
    schema_metadata
        .get()
        .assert_service_revision(GREETER_SERVICE_NAME, 1);

    // Let's register the third time with a new ARN, that's a new lambda
    let arn_2: LambdaARN = "arn:aws:lambda:region:account:function:lambda-function:2"
        .parse()
        .unwrap();
    let register_deployment_request_2 = RegisterDeploymentRequest {
        deployment_address: DeploymentAddress::Lambda(LambdaDeploymentAddress::new(
            arn_2.clone(),
            None,
        )),
        additional_headers: Default::default(),
        metadata: Default::default(),
        use_http_11: false,
        allow_breaking: AllowBreakingChanges::No,
        overwrite: Overwrite::No,
        apply_mode: ApplyMode::Apply,
    };
    let (add_deployment_result, deployment_2, _) = schema_registry
        .register_deployment(register_deployment_request_2.clone())
        .await
        .unwrap();
    assert_eq!(add_deployment_result, AddDeploymentResult::Created);
    assert_eq!(
        deployment_2.ty.as_address(),
        LambdaDeploymentAddress::new(arn_2.clone(), None).into()
    );
    assert_ne!(deployment_2.id, deployment_1.id);
    schema_metadata
        .get()
        .assert_service_revision(GREETER_SERVICE_NAME, 2);

    // Test force with Lambda ARN
    let (add_deployment_result, expected_deployment_2, _) = schema_registry
        .register_deployment(RegisterDeploymentRequest {
            // Overwrite!
            overwrite: Overwrite::Yes,
            ..register_deployment_request_2
        })
        .await
        .unwrap();
    assert_eq!(add_deployment_result, AddDeploymentResult::Overwritten);
    assert_eq!(deployment_2.id, expected_deployment_2.id);
    schema_metadata
        .get()
        .assert_service_revision(GREETER_SERVICE_NAME, 3);
}
