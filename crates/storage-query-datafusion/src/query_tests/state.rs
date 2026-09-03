// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::data::FixtureFactory;
use super::fixture::{QueryExpectation, QueryFixture};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_state_distinct_service_key_and_scope() {
    let factory = FixtureFactory::default();
    let states = [
        factory.create_state(
            Some("scope-a"),
            "TestService",
            "key-1",
            b"state-1",
            b"value-1",
        ),
        factory.create_state(
            Some("scope-a"),
            "TestService",
            "key-1",
            b"state-2",
            b"value-2",
        ),
        factory.create_state(
            Some("scope-b"),
            "TestService",
            "key-2",
            b"state-1",
            b"value-3",
        ),
        factory.create_state(
            Some("scope-b"),
            "OtherService",
            "ignored-key",
            b"state-1",
            b"value-4",
        ),
    ];

    let mut fixture = QueryFixture::create().await;
    fixture
        .populate(|tables| {
            for state in &states {
                tables.state().populate(state)?;
            }
            Ok(())
        })
        .await;

    fixture
        .assert_queries(&[QueryExpectation {
            name: "distinct scoped service instances",
            sql: r#"SELECT DISTINCT service_key, scope
                    FROM state
                    WHERE "service_name" = 'TestService'
                    LIMIT 2"#,
            expected: &[
                "+-------------+---------+",
                "| service_key | scope   |",
                "+-------------+---------+",
                "| key-1       | scope-a |",
                "| key-2       | scope-b |",
                "+-------------+---------+",
            ],
        }])
        .await;
}
