// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use cling::prelude::*;
use comfy_table::Table;
use http::{HeaderName, HeaderValue, StatusCode, Uri};
use indicatif::ProgressBar;
use indoc::indoc;
use restate_admin_rest_model::deployments::{
    DetailedDeploymentResponse, RegisterDeploymentRequest, RegisterDeploymentResponse,
};
use restate_admin_rest_model::version::AdminApiVersion;
use restate_cli_util::ui::console::{Styled, StyledTable, confirm_or_exit};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{c_eprintln, c_error, c_indent_table, c_indentln, c_success, c_warn};
use restate_types::identifiers::LambdaARN;
use restate_types::schema::service::ServiceMetadata;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface, Deployment, MetasClientError};
use crate::console::c_println;
use crate::ui::deployments::render_deployment_url;
use crate::ui::service_handlers::{
    create_service_handlers_table, create_service_handlers_table_diff, icon_for_service_type,
};

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "discover", visible_alias = "add")]
#[cling(run = "run_register")]
pub struct Register {
    /// Allow performing incompatible changes to services, detected during discovery.
    #[clap(long)]
    breaking: bool,

    /// Force overwriting the deployment if it already exists or if incompatible changes were
    /// detected during discovery. When set, implies `--breaking`.
    #[clap(long)]
    force: bool,

    #[clap(long)]
    /// The role ARN that Restate server will assume when invoking any service on the Lambda being
    /// discovered.
    assume_role_arn: Option<String>,

    /// Additional header that will be sent to the endpoint during the discovery request.
    ///
    /// Use `--extra-header name=value` format and repeat --extra-header for each additional header.
    #[clap(long="extra-header", value_parser = parse_header, action = clap::ArgAction::Append)]
    extra_headers: Option<Vec<HeaderKeyValue>>,

    /// Attempt discovery using a client that defaults to HTTP1.1 instead of a prior-knowledge HTTP2 client.
    /// This may be necessary if you see `META0014` discovering local dev servers like `wrangler dev`.
    #[clap(long = "use-http1.1")]
    use_http_11: bool,

    /// The URL or ARN that Restate server needs to fetch service information from.
    ///
    /// The URL must be network-accessible from Restate server. In case of using
    /// Lambda ARN, the ARN should include the function version.
    #[clap(value_parser = parse_deployment)]
    deployment: DeploymentEndpoint,
}

#[derive(Clone)]
struct HeaderKeyValue {
    key: HeaderName,
    value: HeaderValue,
}

#[derive(Clone, Debug)]
enum DeploymentEndpoint {
    Uri(Uri),
    Lambda(LambdaARN),
}

impl DeploymentEndpoint {
    fn cli_parameter_display(&self) -> String {
        match self {
            DeploymentEndpoint::Uri(uri) => uri.to_string(),
            DeploymentEndpoint::Lambda(arn) => arn.to_string(),
        }
    }
}

impl Display for DeploymentEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeploymentEndpoint::Uri(uri) => write!(f, "URL {uri}"),
            DeploymentEndpoint::Lambda(arn) => write!(f, "AWS Lambda ARN {arn}"),
        }
    }
}

fn parse_header(
    raw: &str,
) -> Result<HeaderKeyValue, Box<dyn std::error::Error + Send + Sync + 'static>> {
    // key=value
    let pos = raw
        .find('=')
        .ok_or_else(|| format!("invalid name=value: no `=` found in `{raw}`"))?;
    let key = &raw[..pos];
    let value = &raw[pos + 1..];

    Ok(HeaderKeyValue {
        key: HeaderName::from_str(key)?,
        value: HeaderValue::from_str(value)?,
    })
}

// Needed as a function to allow clap to parse to [`Deployment`]
fn parse_deployment(
    raw: &str,
) -> Result<DeploymentEndpoint, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let deployment = if raw.starts_with("arn:") {
        DeploymentEndpoint::Lambda(LambdaARN::from_str(raw)?)
    } else {
        let mut uri = Uri::from_str(raw).map_err(|e| format!("invalid URL({e})"))?;
        let mut parts = uri.into_parts();
        if parts.scheme.is_none() {
            parts.scheme = Some(http::uri::Scheme::HTTP);
        }
        if parts.path_and_query.is_none() {
            parts.path_and_query = Some(http::uri::PathAndQuery::from_str("/")?);
        }
        uri = Uri::from_parts(parts)?;
        DeploymentEndpoint::Uri(uri)
    };
    Ok(deployment)
}

// NOTE: Without parsing the proto descriptor, we can't detect the details of the
// schema changes. We can only mention additions or removals of services or functions
// and that's probably good enough for now!
pub async fn run_register(State(env): State<CliEnv>, discover_opts: &Register) -> Result<()> {
    let headers = discover_opts.extra_headers.as_ref().map(|headers| {
        HashMap::from_iter(headers.iter().map(|kv| (kv.key.clone(), kv.value.clone())))
    });

    // Preparing the discovery request
    let client = AdminClient::new(&env).await?;

    if discover_opts.breaking && client.admin_api_version < AdminApiVersion::V3 {
        bail!("--breaking is only supported when interacting with Restate >= 1.6");
    }

    let deployment = match &discover_opts.deployment {
        #[cfg(feature = "cloud")]
        DeploymentEndpoint::Uri(uri) if uri.scheme_str() == Some("tunnel") => {
            let environment_info = match (
                &env.config.environment_type,
                &env.config.cloud.environment_info,
            ) {
                (crate::cli_env::EnvironmentType::Cloud, Some(environment_info)) => {
                    environment_info
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "To register tunnel:// URLs, first switch to the Cloud environment you're tunnelling to using `restate config use-environment`"
                    ));
                }
            };

            let unprefixed_environment_id = environment_info
                .environment_id
                .strip_prefix("env_")
                .ok_or(anyhow::anyhow!(
                    "Unexpected environment ID format: {}",
                    environment_info.environment_id
                ))?;

            let authority = uri
                .authority()
                .ok_or(anyhow::anyhow!("tunnel:// URLs must have an authority"))?;

            let port = authority
                .port_u16()
                .ok_or(anyhow::anyhow!("tunnel:// URLs must have a port"))?;

            let proxy_host = &env
                .config
                .cloud
                .proxy_base_url
                .host_str()
                .expect("proxy_base_url must have a host");

            let uri = Uri::builder()
                .scheme(env.config.cloud.proxy_base_url.scheme())
                .authority(format!("{proxy_host}:{port}"))
                .path_and_query(format!("/{unprefixed_environment_id}/{}", authority.host()))
                .build()?;

            DeploymentEndpoint::Uri(uri)
        }
        other => other.clone(),
    };

    let mk_request_body = |breaking, force, dry_run| match &deployment {
        DeploymentEndpoint::Uri(uri) => RegisterDeploymentRequest::Http {
            uri: uri.clone(),
            additional_headers: headers.clone().map(Into::into),
            use_http_11: discover_opts.use_http_11,
            breaking,
            force: Some(force),
            dry_run,
        },
        DeploymentEndpoint::Lambda(arn) => RegisterDeploymentRequest::Lambda {
            arn: arn.to_string(),
            assume_role_arn: discover_opts.assume_role_arn.clone(),
            additional_headers: headers.clone().map(Into::into),
            breaking,
            force: Some(force),
            dry_run,
        },
    };

    if client.admin_api_version >= AdminApiVersion::V3 {
        register_v3_admin_api(discover_opts, client, mk_request_body).await?;
    } else {
        register_v2_admin_api(discover_opts, client, mk_request_body).await?;
    }

    Ok(())
}

async fn register_v3_admin_api(
    discover_opts: &Register,
    client: AdminClient,
    mk_request_body: impl Fn(bool, bool, bool) -> RegisterDeploymentRequest,
) -> Result<()> {
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(format!(
        "Asking restate server at {} for a dry-run discovery of {}",
        &client.base_url, discover_opts.deployment
    ));

    // This fails if the endpoint exists and --force is not set.
    let dry_run_result = client
        // We use force in the dry-run to make sure we get the result of the discovery
        // even if there is it's an existing endpoint
        .discover_deployment(mk_request_body(
            discover_opts.breaking,
            discover_opts.force, /* dry_run = */
            true,
        ))
        .await?;

    if dry_run_result.status_code() == StatusCode::CONFLICT {
        progress.finish_and_clear();
        let api_error = dry_run_result.into_api_error().await?;
        c_println!(
            indoc! {
                "{}
                {}

            ❯ To register a deployment containing breaking changes for a service, use:
                restate deployment register {} --breaking"
            },
            Styled(Style::Danger, "❯ Breaking changes detected:"),
            Styled(Style::Warn, api_error.body),
            discover_opts.deployment.cli_parameter_display()
        );
        bail!("Registration failed");
    }
    if dry_run_result.status_code() == StatusCode::OK && !discover_opts.force {
        progress.finish_and_clear();
        // Admin API V3 returns OK if the deployment already exists and force = false.
        let dry_run_result = dry_run_result.into_body().await?;
        c_println!(
            indoc! {
                "❯ Deployment already exists with id {}
                   No changes will be made.

            ❯ To overwrite this deployment during development, use:
                restate deployment register {} --force

            ❯ To modify connection parameters, use:
                restate deployment edit {}"
            },
            Styled(Style::Info, &dry_run_result.id),
            discover_opts.deployment.cli_parameter_display(),
            dry_run_result.id
        );
        return Ok(());
    }
    // At this point, if the deployment exists, StatusCode == OK and force = true
    let deployment_exists = dry_run_result.status_code() == StatusCode::OK;

    let dry_run_response = dry_run_result.into_body().await?;

    progress.finish_and_clear();

    let existing_deployment = if deployment_exists {
        Some(
            client
                .get_deployment(&dry_run_response.id.to_string())
                .await?
                .into_body()
                .await
                .with_context(|| format!("Failed to get deployment {}", dry_run_response.id))?,
        )
    } else {
        None
    };

    if let Some(ref existing_deployment) = existing_deployment {
        c_eprintln!();
        c_warn!(
            "This deployment is already known to the server under the ID \"{}\". \
            Confirming this operation will overwrite services defined by the existing \
            deployment. Inflight invocations to this deployment might move to an unrecoverable \
            failure state afterwards!.\
            \n\nThis is a DANGEROUS operation! \n
            In production, we recommend creating a new deployment with a unique endpoint while \
            keeping the old one active until the old deployment is drained.",
            existing_deployment.id()
        );
        c_eprintln!();
    }

    print_registration_changes(&client, dry_run_response, existing_deployment).await?;

    confirm_or_exit("Are you sure you want to apply those changes?")?;

    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(format!(
        "Asking restate server {} to confirm this deployment (at {})",
        &client.base_url, discover_opts.deployment
    ));

    let registration_result = client
        .discover_deployment(mk_request_body(
            discover_opts.breaking,
            discover_opts.force,
            /* dry_run = */ false,
        ))
        .await?
        .into_body()
        .await?;

    progress.finish_and_clear();
    // print the result of the discovery
    c_success!("DEPLOYMENT:");
    let mut table = Table::new_styled();
    table.set_styled_header(vec!["SERVICE", "REV"]);
    for svc in registration_result.services {
        table.add_row(vec![svc.name, svc.revision.to_string()]);
    }
    c_println!("{}", table);

    Ok(())
}

async fn register_v2_admin_api(
    discover_opts: &Register,
    client: AdminClient,
    mk_request_body: impl Fn(bool, bool, bool) -> RegisterDeploymentRequest,
) -> Result<()> {
    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(format!(
        "Asking restate server at {} for a dry-run discovery of {}",
        &client.base_url, discover_opts.deployment
    ));

    // This fails if the endpoint exists and --force is not set.
    let dry_run_result = client
        // We use force in the dry-run to make sure we get the result of the discovery
        // even if there is it's an existing endpoint
        .discover_deployment(mk_request_body(
            /* breaking */ true, /* force = */ true, /* dry_run = */ true,
        ))
        .await?
        .into_body()
        .await?;

    progress.finish_and_clear();

    // Is this an existing deployment?
    let existing_deployment = match client
        .get_deployment(&dry_run_result.id.to_string())
        .await?
        .into_body()
        .await
    {
        Ok(existing_deployment) => {
            // Appears to be an existing endpoint.
            Some(existing_deployment)
        }
        Err(MetasClientError::Api(err)) if err.http_status_code == StatusCode::NOT_FOUND => None,
        // We cannot get existing deployment details. This is a problem.
        Err(err) => return Err(err.into()),
    };

    if let Some(ref existing_deployment) = existing_deployment {
        if !discover_opts.force {
            bail!(
                "A deployment already exists that uses this endpoint (ID: {}). Use --force to overwrite it.",
                existing_deployment.id(),
            )
        } else {
            c_eprintln!();
            c_warn!(
                "This deployment is already known to the server under the ID \"{}\". \
                Confirming this operation will overwrite services defined by the existing \
                deployment. Inflight invocations to this deployment might move to an unrecoverable \
                failure state afterwards!.\
                \n\nThis is a DANGEROUS operation! \n
                In production, we recommend creating a new deployment with a unique endpoint while \
                keeping the old one active until the old deployment is drained.",
                existing_deployment.id()
            );
            c_eprintln!();
        }
    }

    print_registration_changes(&client, dry_run_result, existing_deployment).await?;

    confirm_or_exit("Are you sure you want to apply those changes?")?;

    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(format!(
        "Asking restate server {} to confirm this deployment (at {})",
        &client.base_url, discover_opts.deployment
    ));

    let registration_result = client
        .discover_deployment(mk_request_body(
            discover_opts.breaking,
            discover_opts.force,
            /* dry_run = */ false,
        ))
        .await?
        .into_body()
        .await?;

    progress.finish_and_clear();
    // print the result of the discovery
    c_success!("DEPLOYMENT:");
    let mut table = Table::new_styled();
    table.set_styled_header(vec!["SERVICE", "REV"]);
    for svc in registration_result.services {
        table.add_row(vec![svc.name, svc.revision.to_string()]);
    }
    c_println!("{}", table);

    Ok(())
}

async fn print_registration_changes(
    client: &AdminClient,
    dry_run_result: RegisterDeploymentResponse,
    existing_deployment: Option<DetailedDeploymentResponse>,
) -> Result<()> {
    let discovered_service_names = dry_run_result
        .services
        .iter()
        .map(|service| service.name.clone())
        .collect::<HashSet<_>>();

    // Services found in this discovery
    let (added, updated): (Vec<_>, Vec<_>) = dry_run_result
        .services
        .iter()
        .partition(|svc| svc.revision == 1);

    c_println!(
        "Deployment ID:  {}",
        Styled(Style::Info, &dry_run_result.id)
    );
    // The following services will be added:
    if !added.is_empty() {
        c_println!();
        c_println!(
            "❯ SERVICES THAT WILL BE {}:",
            Styled(Style::Success, "ADDED")
        );
        for service in added {
            c_indentln!(1, "- {}", Styled(Style::Success, &service.name),);
            c_indentln!(
                2,
                "Type: {:?} {}",
                service.ty,
                icon_for_service_type(&service.ty),
            );

            c_indent_table!(2, create_service_handlers_table(service.handlers.values()));
            c_println!();
        }
        c_println!();
    }

    // The following services will be updated:
    if !updated.is_empty() {
        c_println!();
        // used to resolving old deployments.
        let mut deployment_cache: HashMap<String, Deployment> = HashMap::new();
        // A single spinner spans all requests.
        let progress = ProgressBar::new_spinner();
        progress.set_style(
            indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap(),
        );
        progress.enable_steady_tick(std::time::Duration::from_millis(120));

        let mut existing_services: HashMap<String, ServiceMetadata> = HashMap::new();
        for service in &updated {
            // Get the current service information by querying the server.
            progress.set_message(format!(
                "Fetching information about service '{}'",
                service.name,
            ));
            match client.get_service(&service.name).await?.into_body().await {
                Ok(service_metadata) => {
                    existing_services.insert(service.name.clone(), service_metadata);
                }
                Err(e) => {
                    // Let the spinner pause to print the error.
                    progress.suspend(|| {
                        c_eprintln!(
                        "Warning: Couldn't fetch information about service {} from Restate server. \
                         We will not be able to show the detailed changes for this service.",
                        service.name,
                    );
                        c_error!("{}", e);
                    });
                }
            };
        }
        progress.finish_and_clear();

        c_println!(
            "❯ SERVICES THAT WILL BE {}:",
            Styled(Style::Warn, "UPDATED")
        );
        for svc in updated {
            c_indentln!(1, "- {}", Styled(Style::Info, &svc.name),);
            c_indentln!(2, "Type: {:?} {}", svc.ty, icon_for_service_type(&svc.ty),);

            if let Some(existing_svc) = existing_services.get(&svc.name) {
                c_indentln!(
                    2,
                    "Revision: {} -> {}",
                    &existing_svc.revision,
                    Styled(Style::Success, &svc.revision),
                );
                if existing_svc.deployment_id != dry_run_result.id {
                    let maybe_old_deployment = resolve_deployment(
                        client,
                        &mut deployment_cache,
                        &existing_svc.deployment_id.to_string(),
                    )
                    .await;
                    let old_deployment_message = maybe_old_deployment
                        .map(|old_endpoint| render_deployment_url(&old_endpoint));
                    c_indentln!(
                        2,
                        "Old Deployment: {} (at {})",
                        old_deployment_message.as_deref().unwrap_or("<UNKNOWN>"),
                        &existing_svc.deployment_id,
                    );
                }

                let tt = create_service_handlers_table_diff(
                    existing_svc.handlers.values(),
                    svc.handlers.values(),
                );
                c_indent_table!(2, tt);
            } else {
                c_indentln!(
                    2,
                    "{}",
                    create_service_handlers_table(svc.handlers.values())
                );
            }
            c_println!();
        }
        c_println!();
    }

    // The following services will be removed/forgotten:
    if let Some(existing_deployment) = existing_deployment {
        let (_, _, services) = Deployment::from_detailed_deployment_response(existing_deployment);

        // The following services will be removed/forgotten:
        let services_removed = services
            .iter()
            .filter(|service| !discovered_service_names.contains(&service.name))
            .collect::<Vec<_>>();
        if !services_removed.is_empty() {
            c_println!();
            c_println!(
                "❯ SERVICES THAT WILL BE {}:",
                Styled(Style::Danger, "REMOVED")
            );
            for svc in services_removed {
                c_indentln!(2, "- {}", Styled(Style::Danger, &svc.name));
            }
            c_println!();
        }
    }
    Ok(())
}

async fn resolve_deployment(
    client: &AdminClient,
    cache: &mut HashMap<String, Deployment>,
    deployment_id: &str,
) -> Option<Deployment> {
    if cache.contains_key(deployment_id) {
        return cache.get(deployment_id).cloned();
    }

    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(format!(
        "Fetching information about existing deployments at {deployment_id}",
    ));

    let deployment = client
        .get_deployment(deployment_id)
        .await
        .ok()?
        .into_body()
        .await
        .ok()
        .map(|endpoint| {
            let (_, deployment, _) = Deployment::from_detailed_deployment_response(endpoint);
            cache.insert(deployment_id.to_string(), deployment.clone());
            Some(deployment)
        })?;
    progress.finish_and_clear();

    deployment
}
