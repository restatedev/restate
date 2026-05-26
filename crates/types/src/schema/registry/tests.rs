// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

#[cfg(test)]
mod http_auth_validation_tests {
    use super::super::{HttpAuthValidationError, effective_http_patch_inputs, validate_http_auth};
    use crate::deployment::Headers;
    use http::{HeaderName, HeaderValue, Uri};

    fn assert_invalid_field(result: Result<(), HttpAuthValidationError>, expected_field: &str) {
        match result {
            Err(err) => assert_eq!(err.field, expected_field),
            Ok(()) => panic!("expected InvalidField({expected_field}), got Ok"),
        }
    }

    #[test]
    fn rejects_non_https_public_host() {
        let uri: Uri = "http://example.com/".parse().unwrap();
        assert_invalid_field(validate_http_auth(&uri, None), "auth");
    }

    #[test]
    fn accepts_https_public_host() {
        let uri: Uri = "https://svc.example.com/".parse().unwrap();
        validate_http_auth(&uri, None).expect("https public host accepted");
    }

    #[test]
    fn accepts_non_https_loopback() {
        for host in ["localhost", "127.0.0.1", "[::1]", "10.0.0.1", "[fc00::1]"] {
            let uri: Uri = format!("http://{host}/").parse().unwrap();
            validate_http_auth(&uri, None)
                .unwrap_or_else(|e| panic!("expected accept for {host}, got {e:?}"));
        }
    }

    #[test]
    fn rejects_x_serverless_authorization_header() {
        let uri: Uri = "https://svc.example.com/".parse().unwrap();
        let mut headers: Headers = Headers::new();
        headers.insert(
            HeaderName::from_static("x-serverless-authorization"),
            HeaderValue::from_static("Bearer y"),
        );

        assert_invalid_field(
            validate_http_auth(&uri, Some(&headers)),
            "additional_headers",
        );
    }

    #[test]
    fn rejects_x_serverless_authorization_alongside_authorization() {
        let uri: Uri = "https://svc.example.com/".parse().unwrap();
        let mut headers: Headers = Headers::new();
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer x"),
        );
        headers.insert(
            HeaderName::from_static("x-serverless-authorization"),
            HeaderValue::from_static("Bearer y"),
        );

        assert_invalid_field(
            validate_http_auth(&uri, Some(&headers)),
            "additional_headers",
        );
    }

    #[test]
    fn accepts_single_authorization_header() {
        let uri: Uri = "https://svc.example.com/".parse().unwrap();
        let mut headers: Headers = Headers::new();
        headers.insert(
            HeaderName::from_static("authorization"),
            HeaderValue::from_static("Bearer x"),
        );

        validate_http_auth(&uri, Some(&headers)).expect("single Authorization header is allowed");
    }

    #[test]
    fn patch_validation_rejects_http_uri_change_when_auth_persisted() {
        let existing_uri: Uri = "https://svc.example.com/".parse().unwrap();
        let existing_headers = Headers::new();
        let new_uri: Uri = "http://attacker.example.com/".parse().unwrap();

        let (effective_uri, effective_headers) =
            effective_http_patch_inputs(Some(&new_uri), None, &existing_uri, &existing_headers);

        assert_invalid_field(
            validate_http_auth(effective_uri, Some(effective_headers.as_ref())),
            "auth",
        );
    }

    #[test]
    fn patch_validation_rejects_added_x_serverless_authorization_header() {
        let existing_uri: Uri = "https://svc.example.com/".parse().unwrap();
        let existing_headers = Headers::new();

        let mut patched = Headers::new();
        patched.insert(
            HeaderName::from_static("x-serverless-authorization"),
            HeaderValue::from_static("Bearer attacker"),
        );

        let (effective_uri, effective_headers) =
            effective_http_patch_inputs(None, Some(&patched), &existing_uri, &existing_headers);

        assert_invalid_field(
            validate_http_auth(effective_uri, Some(effective_headers.as_ref())),
            "additional_headers",
        );
    }

    #[test]
    fn patch_validation_accepts_noop_against_persisted_safe_record() {
        let existing_uri: Uri = "https://svc.example.com/".parse().unwrap();
        let existing_headers = Headers::new();

        let (effective_uri, effective_headers) =
            effective_http_patch_inputs(None, None, &existing_uri, &existing_headers);

        validate_http_auth(effective_uri, Some(effective_headers.as_ref()))
            .expect("no-op PATCH against safe persisted record is accepted");
    }
}

#[test(tokio::test)]
pub async fn register_http_with_gcp_auth_persists_audience_verbatim() {
    // The registry no longer rewrites `auth`; the wire-to-persisted conversion at the REST
    // boundary is the only place that materialises the audience. This test confirms the
    // registry round-trips the persisted record without mutation.
    use crate::deployment::{GoogleIdTokenAuth, HttpAuth, HttpDeploymentAddress};
    use bytestring::ByteString;
    use http::Uri;

    let schema_metadata = mock_arc_schema();
    let schema_registry = SchemaRegistry::new(
        schema_metadata.clone(),
        DiscoveryResponse::mock(vec![greeter_service()]),
        (),
    );

    let uri: Uri = "https://api.acme.com/svc".parse().unwrap();
    let explicit_audience = ByteString::from_static("https://svc-abc-uc.a.run.app");
    let (_, deployment, _) = schema_registry
        .register_deployment(RegisterDeploymentRequest {
            deployment_address: HttpDeploymentAddress::new(uri)
                .with_auth(Some(HttpAuth::GoogleIdToken(GoogleIdTokenAuth::new(
                    explicit_audience.clone(),
                    None,
                ))))
                .into(),
            additional_headers: Default::default(),
            metadata: Default::default(),
            use_http_11: false,
            allow_breaking: AllowBreakingChanges::No,
            overwrite: Overwrite::No,
            apply_mode: ApplyMode::Apply,
        })
        .await
        .unwrap();

    let DeploymentType::Http {
        auth: Some(HttpAuth::GoogleIdToken(persisted)),
        ..
    } = deployment.ty
    else {
        panic!("expected persisted GoogleIdToken auth");
    };
    assert_eq!(
        persisted.audience(),
        &explicit_audience,
        "audience must be preserved verbatim by the registry",
    );
}
