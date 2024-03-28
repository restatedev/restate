// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use reqwest::header::ACCEPT;
use restate_admin::service::AdminService;
use restate_bifrost::Bifrost;
use restate_core::TaskKind;
use restate_core::TestCoreEnv;
use restate_meta::MetaService;
use restate_node_services::node_svc::node_svc_client::NodeSvcClient;
use restate_schema_api::subscription::Subscription;
use restate_types::identifiers::SubscriptionId;
use restate_types::invocation::InvocationTermination;
use restate_types::retries::RetryPolicy;
use restate_types::state_mut::ExternalStateMutation;
use restate_worker_api::Error;
use schemars::gen::SchemaSettings;
use std::env;
use std::time::Duration;
use tonic::transport::{Channel, Uri};

fn generate_config_schema() -> anyhow::Result<()> {
    let schema = SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<restate_server::Configuration>();
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}

fn generate_default_config() -> anyhow::Result<()> {
    println!(
        "{}",
        serde_yaml::to_string(&restate_server::Configuration::default())?
    );
    Ok(())
}

// Need this for worker Handle
#[derive(Clone)]
struct Mock;

impl restate_worker_api::Handle for Mock {
    async fn terminate_invocation(
        &self,
        _: InvocationTermination,
    ) -> Result<(), restate_worker_api::Error> {
        Ok(())
    }

    async fn external_state_mutation(&self, _mutation: ExternalStateMutation) -> Result<(), Error> {
        Ok(())
    }
}

impl restate_worker_api::SubscriptionController for Mock {
    async fn start_subscription(&self, _: Subscription) -> Result<(), restate_worker_api::Error> {
        Ok(())
    }

    async fn stop_subscription(&self, _: SubscriptionId) -> Result<(), restate_worker_api::Error> {
        Ok(())
    }

    async fn update_subscriptions(
        &self,
        _: Vec<Subscription>,
    ) -> Result<(), restate_worker_api::Error> {
        Ok(())
    }
}

impl restate_schema_api::subscription::SubscriptionValidator for Mock {
    type Error = restate_worker_api::Error;

    fn validate(&self, _: Subscription) -> Result<Subscription, Self::Error> {
        unimplemented!()
    }
}

async fn generate_rest_api_doc() -> anyhow::Result<()> {
    let admin_options = restate_admin::Options::default();
    let meta_options = restate_meta::Options::default();
    let mut meta =
        MetaService::from_options(meta_options, Mock).expect("expect to build meta service");
    let openapi_address = format!(
        "http://localhost:{}/openapi",
        admin_options.bind_address.port()
    );
    let admin_service = AdminService::from_options(
        admin_options,
        meta.schemas(),
        meta.meta_handle(),
        meta.schema_reader(),
    );
    meta.init().await.unwrap();

    // We start the Meta component, then download the openapi schema generated
    let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
    let bifrost = node_env
        .tc
        .run_in_scope("bifrost init", None, Bifrost::new_in_memory())
        .await;

    node_env.tc.spawn(
        TaskKind::TestRunner,
        "doc-gen",
        None,
        admin_service.run(
            NodeSvcClient::new(Channel::builder(Uri::default()).connect_lazy()),
            bifrost,
        ),
    )?;

    let res = RetryPolicy::fixed_delay(Duration::from_millis(100), 20)
        .retry(|| async {
            reqwest::Client::builder()
                .build()?
                .get(openapi_address.clone())
                .header(ACCEPT, "application/json")
                .send()
                .await?
                .text()
                .await
        })
        .await
        .unwrap();

    println!("{}", res);

    node_env.tc.shutdown_node("completed", 0).await;

    Ok(())
}

fn print_help() {
    println!(
        "
Usage: Run with `cargo xtask <task>`, eg. `cargo xtask generate-config-schema`.
Tasks:
    generate-config-schema: Generate config schema for restate configuration.
    generate-default-config: Generate default configuration.
    generate-rest-api-doc: Generate Rest API documentation. Make sure to have the port 8081 open.
"
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let task = env::args().nth(1);
    match task {
        None => print_help(),
        Some(t) => match t.as_str() {
            "generate-config-schema" => generate_config_schema()?,
            "generate-default-config" => generate_default_config()?,
            "generate-rest-api-doc" => generate_rest_api_doc().await?,
            invalid => {
                print_help();
                bail!("Invalid task name: {}", invalid)
            }
        },
    };
    Ok(())
}
