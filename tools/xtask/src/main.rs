// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::pending;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::{env, io};

use anyhow::bail;
use reqwest::header::ACCEPT;
use schemars::r#gen::SchemaSettings;

use restate_admin::service::AdminService;
use restate_bifrost::Bifrost;
use restate_core::{TaskCenter, TaskCenterBuilder, TestCoreEnv};
use restate_core::{TaskCenterFutureExt, TaskKind};
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_service_protocol::discovery::ServiceDiscovery;
use restate_storage_query_datafusion::table_docs;
use restate_types::config::Configuration;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId, SubscriptionId};
use restate_types::invocation::client::{
    AttachInvocationResponse, CancelInvocationResponse, GetInvocationOutputResponse,
    InvocationClient, InvocationClientError, InvocationOutput, KillInvocationResponse,
    PatchDeploymentId, PurgeInvocationResponse, RestartAsNewInvocationResponse,
    ResumeInvocationResponse, SubmittedInvocationNotification,
};
use restate_types::invocation::{
    InvocationQuery, InvocationRequest, InvocationResponse, InvocationTermination,
};
use restate_types::journal_v2::{EntryIndex, Signal};
use restate_types::live::Constant;
use restate_types::retries::RetryPolicy;
use restate_types::schema::subscriptions::Subscription;
use restate_types::state_mut::ExternalStateMutation;
use restate_worker::SubscriptionController;
use restate_worker::WorkerHandle;
use restate_worker::WorkerHandleError;

fn generate_config_schema() -> anyhow::Result<()> {
    let schema = SchemaSettings::draft2019_09()
        .into_generator()
        .into_root_schema_for::<Configuration>();
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}

fn generate_default_config() {
    println!(
        "{}",
        Configuration::default()
            .dump()
            .expect("default configuration")
    );
}

// Need this for worker Handle
#[derive(Clone)]
struct Mock;

impl WorkerHandle for Mock {
    async fn terminate_invocation(
        &self,
        _: InvocationTermination,
    ) -> Result<(), WorkerHandleError> {
        Ok(())
    }

    async fn external_state_mutation(
        &self,
        _mutation: ExternalStateMutation,
    ) -> Result<(), WorkerHandleError> {
        Ok(())
    }
}

impl SubscriptionController for Mock {
    async fn start_subscription(&self, _: Subscription) -> Result<(), WorkerHandleError> {
        Ok(())
    }

    async fn stop_subscription(&self, _: SubscriptionId) -> Result<(), WorkerHandleError> {
        Ok(())
    }

    async fn update_subscriptions(&self, _: Vec<Subscription>) -> Result<(), WorkerHandleError> {
        Ok(())
    }
}

impl InvocationClient for Mock {
    fn append_invocation_and_wait_submit_notification(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: Arc<InvocationRequest>,
    ) -> impl Future<Output = Result<SubmittedInvocationNotification, InvocationClientError>> + Send
    {
        pending()
    }

    fn append_invocation_and_wait_output(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: Arc<InvocationRequest>,
    ) -> impl Future<Output = Result<InvocationOutput, InvocationClientError>> + Send {
        pending()
    }

    fn attach_invocation(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationQuery,
    ) -> impl Future<Output = Result<AttachInvocationResponse, InvocationClientError>> + Send {
        pending()
    }

    fn get_invocation_output(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationQuery,
    ) -> impl Future<Output = Result<GetInvocationOutputResponse, InvocationClientError>> + Send
    {
        pending()
    }

    fn append_invocation_response(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationResponse,
    ) -> impl Future<Output = Result<(), InvocationClientError>> + Send {
        pending()
    }

    fn append_signal(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationId,
        _: Signal,
    ) -> impl Future<Output = Result<(), InvocationClientError>> + Send {
        pending()
    }

    fn cancel_invocation(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationId,
    ) -> impl Future<Output = Result<CancelInvocationResponse, InvocationClientError>> + Send {
        pending()
    }

    fn kill_invocation(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationId,
    ) -> impl Future<Output = Result<KillInvocationResponse, InvocationClientError>> + Send {
        pending()
    }

    fn purge_invocation(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationId,
    ) -> impl Future<Output = Result<PurgeInvocationResponse, InvocationClientError>> + Send {
        pending()
    }

    fn purge_journal(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationId,
    ) -> impl Future<Output = Result<PurgeInvocationResponse, InvocationClientError>> + Send {
        pending()
    }

    fn restart_as_new_invocation(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationId,
        _: EntryIndex,
        _: PatchDeploymentId,
    ) -> impl Future<Output = Result<RestartAsNewInvocationResponse, InvocationClientError>> + Send
    {
        pending()
    }

    fn resume_invocation(
        &self,
        _: PartitionProcessorRpcRequestId,
        _: InvocationId,
        _: PatchDeploymentId,
    ) -> impl Future<Output = Result<ResumeInvocationResponse, InvocationClientError>> + Send {
        pending()
    }
}

async fn generate_rest_api_doc() -> anyhow::Result<()> {
    let config = Configuration::default();
    let openapi_address = format!(
        "http://localhost:{}/openapi",
        config.admin.bind_address.port()
    );

    // We start the Meta service, then download the openapi schema generated
    let node_env = TestCoreEnv::create_with_single_node(1, 1).await;
    let bifrost = Bifrost::init_in_memory(node_env.metadata_writer.clone()).await;

    let admin_service = AdminService::new(
        node_env.metadata_writer.clone(),
        bifrost,
        Mock,
        ServiceDiscovery::new(
            RetryPolicy::default(),
            ServiceClient::from_options(&config.common.service_client, AssumeRoleCacheMode::None)
                .unwrap(),
        ),
        None,
    );

    TaskCenter::spawn(
        TaskKind::TestRunner,
        "doc-gen",
        admin_service.run(Constant::new(config.admin)),
    )?;

    let res = RetryPolicy::fixed_delay(Duration::from_millis(100), Some(20))
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

    println!("{res}");

    Ok(())
}

fn render_table_docs(mut write: impl Write) -> io::Result<()> {
    for table_doc in restate_storage_query_datafusion::table_docs::ALL_TABLE_DOCS {
        render_table_doc(table_doc, &mut write)?;
    }

    // sys_invocation is a view which was not registered at table_docs::TABLE_DOCS
    render_table_doc(&table_docs::sys_invocation_table_docs(), &mut write)?;

    Ok(())
}

fn render_table_doc(table_doc: &impl table_docs::TableDocs, w: &mut impl Write) -> io::Result<()> {
    writeln!(w, "## Table: `{}`\n", table_doc.name())?;
    writeln!(w, "{}\n", table_doc.description())?;
    writeln!(w, "| Column name | Type | Description |")?;
    writeln!(w, "|-------------|------|-------------|")?;
    for column in table_doc.columns() {
        writeln!(
            w,
            "| `{}` | `{}` | {} |",
            column.name,
            column.column_type,
            column.description.trim()
        )?;
    }
    writeln!(w)?;

    Ok(())
}

fn generate_table_docs() -> anyhow::Result<()> {
    let mut dest = io::stdout();

    // File header
    writeln!(
        &mut dest,
        r"# SQL Introspection API

This page contains the reference of the introspection tables.
To learn how to access the introspection interface, check out the [introspection documentation](/operate/introspection).
"
    )?;

    render_table_docs(&mut dest)?;

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
    generate-table-docs: Generate default configuration.
"
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("building task-center should not fail")
        .into_handle();
    let task = env::args().nth(1);
    match task {
        None => print_help(),
        Some(t) => match t.as_str() {
            "generate-config-schema" => generate_config_schema()?,
            "generate-default-config" => generate_default_config(),
            "generate-rest-api-doc" => {
                generate_rest_api_doc()
                    .in_tc_as_task(&tc, TaskKind::Background, "generate-rest-api-doc")
                    .await?
            }
            "generate-table-docs" => generate_table_docs()?,
            invalid => {
                print_help();
                bail!("Invalid task name: {}", invalid)
            }
        },
    };
    tc.shutdown_node("completed", 0).await;
    Ok(())
}
