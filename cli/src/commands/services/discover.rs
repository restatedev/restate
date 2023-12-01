// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use crate::cli_env::CliEnv;
use crate::console::c_println;
use crate::meta_client::{MetaClient, MetaClientError, MetaClientInterface};
use crate::ui::console::{confirm_or_exit, Styled, StyledTable};
use crate::ui::render::render_endpoint_url;
use crate::ui::service_methods::{
    create_service_methods_table, create_service_methods_table_diff, icon_for_service_flavor,
};
use crate::ui::stylesheet::Style;
use crate::{c_eprintln, c_error, c_indent_table, c_indentln, c_success, c_warn};

use http::{HeaderName, HeaderValue, StatusCode, Uri};
use restate_meta_rest_model::endpoints::{
    LambdaARN, RegisterServiceEndpointMetadata, RegisterServiceEndpointRequest, ServiceEndpoint,
};

use anyhow::Result;
use cling::prelude::*;
use comfy_table::Table;
use indicatif::ProgressBar;
use restate_meta_rest_model::services::ServiceMetadata;

#[derive(Run, Parser, Collect, Clone)]
#[clap(visible_alias = "register", visible_alias = "add")]
#[cling(run = "run_discover")]
pub struct Discover {
    /// Force overwriting an endpoint if it already exists or if incompatible changes were
    /// detected during discovery.
    #[clap(long)]
    force: bool,

    #[clap(long)]
    /// The role ARN that Restate server will assume when invoking any service on the Lambda being
    /// discovered.
    assume_role_arn: Option<String>,

    /// Additional header that will be sent to the endpoint during the discovery request.
    ///
    /// Use `-e name=value` format and repeat -e for each additional header.
    #[clap(long="extra-header", short, value_parser = parse_header, action = clap::ArgAction::Append)]
    extra_headers: Option<Vec<HeaderKeyValue>>,

    /// The URL or ARN that Restate server needs to fetch service information from.
    ///
    /// The URL must be network-accessible from Restate server. In case of using
    /// Lambda ARN, the ARN should include the function version.
    #[clap(value_parser = parse_endpoint)]
    endpoint: Endpoint,
}

#[derive(Clone)]
struct HeaderKeyValue {
    key: HeaderName,
    value: HeaderValue,
}

#[derive(Clone, Debug)]
enum Endpoint {
    Uri(Uri),
    Lambda(LambdaARN),
}

impl Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Uri(uri) => write!(f, "URL {}", uri),
            Endpoint::Lambda(arn) => write!(f, "AWS Lambda ARN {}", arn),
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

// Needed as a function to allow clap to parse to [`Endpoint`]
fn parse_endpoint(
    raw: &str,
) -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let endpoint = if raw.starts_with("arn:") {
        Endpoint::Lambda(LambdaARN::from_str(raw)?)
    } else {
        let uri = Uri::from_str(raw).map_err(|e| format!("invalid URL({e})"))?;
        Endpoint::Uri(uri)
    };
    Ok(endpoint)
}

// NOTE: Without parsing the proto descriptor, we can't detect the details of the
// schema changes. We can only mention additions or removals of services or functions
// and that's probably good enough for now!
#[cling_handler]
pub async fn run_discover(State(env): State<CliEnv>, discover_opts: &Discover) -> Result<()> {
    let headers = discover_opts.extra_headers.as_ref().map(|headers| {
        HashMap::from_iter(headers.iter().map(|kv| (kv.key.clone(), kv.value.clone())))
    });

    // Preparing the discovery request
    let client = crate::meta_client::MetaClient::new(&env)?;
    let endpoint_metadata = match &discover_opts.endpoint {
        Endpoint::Uri(uri) => RegisterServiceEndpointMetadata::Http { uri: uri.clone() },
        Endpoint::Lambda(arn) => RegisterServiceEndpointMetadata::Lambda {
            arn: arn.to_string(),
            assume_role_arn: discover_opts.assume_role_arn.clone(),
        },
    };

    let mk_request_body = |force, dry_run| RegisterServiceEndpointRequest {
        endpoint_metadata: endpoint_metadata.clone(),
        additional_headers: headers.clone().map(Into::into),
        force,
        dry_run,
    };

    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(format!(
        "Asking restate server at {} for a dry-run discovery of {}",
        env.meta_base_url, discover_opts.endpoint
    ));

    // This fails if the endpoint exists and --force is not set.
    let dry_run_result = client
        // We use force in the dry-run to make sure we get the result of the discovery
        // even if there is it's an existing endpoint
        .discover_endpoint(mk_request_body(
            /* force = */ true, /* dry_run = */ true,
        ))
        .await?
        .into_body()
        .await?;

    progress.finish_and_clear();

    // Is this an existing endpoint?
    let existing_endpoint = match client
        .get_endpoint(&dry_run_result.id)
        .await?
        .into_body()
        .await
    {
        Ok(existing_endpoint) => {
            // Appears to be an existing endpoint.
            Some(existing_endpoint)
        }
        Err(MetaClientError::Api(err)) if err.http_status_code == StatusCode::NOT_FOUND => None,
        // We cannot get existing endpoint details. This is a problem.
        Err(err) => return Err(err.into()),
    };

    if let Some(ref existing_endpoint) = existing_endpoint {
        if !discover_opts.force {
            c_error!(
                "Endpoint already exists with id {}. Use --force to overwrite it.",
                existing_endpoint.id,
            );
            return Ok(());
        } else {
            c_eprintln!();
            c_warn!("This endpoint is already known to the server under the ID \"{}\". \
                Applying this discovery will overwrite services defined by the previous version of this endpoint. \
                Inflight invocations to this endpoint might move to an unrecoverable \
                failure state after the forced replacement.\
                \n\nThis is a DANGEROUS operation! \n
                In production, we recommend deploying a new endpoint while keeping the old one active \
                until the old endpoint is fully drained.", existing_endpoint.id
            );
            c_eprintln!();
        }
    }

    let discovered_service_names = dry_run_result
        .services
        .iter()
        .map(|svc| svc.name.clone())
        .collect::<HashSet<_>>();

    // Services found in this discovery
    let (added, updated): (Vec<_>, Vec<_>) = dry_run_result
        .services
        .iter()
        .partition(|svc| svc.revision == 1);

    c_println!("Endpoint ID:  {}", Styled(Style::Info, &dry_run_result.id));
    // The following services will be added:
    if !added.is_empty() {
        c_println!();
        c_println!(
            "❯ SERVICES THAT WILL BE {}:",
            Styled(Style::Success, "ADDED")
        );
        for svc in added {
            c_indentln!(1, "- {}", Styled(Style::Success, &svc.name),);
            c_indentln!(
                2,
                "Type: {:?} {}",
                svc.instance_type,
                icon_for_service_flavor(&svc.instance_type),
            );

            c_indent_table!(
                2,
                create_service_methods_table(
                    &env.ui_config,
                    svc.instance_type.clone(),
                    &svc.methods
                )
            );
            c_println!();
        }
        c_println!();
    }

    // The following services will be updated:
    if !updated.is_empty() {
        c_println!();
        // used to resolving old endpoints.
        let mut endpoint_cache: HashMap<String, ServiceEndpoint> = HashMap::new();
        // A single spinner spans all requests.
        let progress = ProgressBar::new_spinner();
        progress.set_style(
            indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap(),
        );
        progress.enable_steady_tick(std::time::Duration::from_millis(120));

        let mut existing_services: HashMap<String, ServiceMetadata> = HashMap::new();
        for svc in &updated {
            // Get the current service information by querying the server.
            progress.set_message(format!(
                "Fetching information about existing service {}",
                svc.name,
            ));
            match client.get_service(&svc.name).await?.into_body().await {
                Ok(svc_metadata) => {
                    existing_services.insert(svc.name.clone(), svc_metadata);
                }
                Err(e) => {
                    // Let the spinner pause to print the error.
                    progress.suspend(|| {
                        c_eprintln!(
                        "Warning: Couldn't fetch information about service {} from Restate server. \
                         We will not be able to show the detailed changes for this service.",
                        svc.name,
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
            c_indentln!(
                2,
                "Type: {:?} {}",
                svc.instance_type,
                icon_for_service_flavor(&svc.instance_type),
            );

            if let Some(existing_svc) = existing_services.get(&svc.name) {
                c_indentln!(
                    2,
                    "Revision: {} -> {}",
                    &existing_svc.revision,
                    Styled(Style::Success, &svc.revision),
                );
                if existing_svc.endpoint_id != dry_run_result.id {
                    let maybe_old_endpoint =
                        resolve_endpoint(&client, &mut endpoint_cache, &existing_svc.endpoint_id)
                            .await;
                    let old_endpoint_message =
                        maybe_old_endpoint.map(|old_endpoint| render_endpoint_url(&old_endpoint));
                    c_indentln!(
                        2,
                        "Old Endpoint: {} ({})",
                        old_endpoint_message.as_deref().unwrap_or("<UNKNOWN>"),
                        &existing_svc.endpoint_id,
                    );
                }

                let tt = create_service_methods_table_diff(
                    &env.ui_config,
                    svc.instance_type.clone(),
                    &existing_svc.methods,
                    &svc.methods,
                );
                c_indent_table!(2, tt);
            } else {
                c_indentln!(
                    2,
                    "{}",
                    create_service_methods_table(
                        &env.ui_config,
                        svc.instance_type.clone(),
                        &svc.methods
                    )
                );
            }
            c_println!();
        }
        c_println!();
    }

    // The following services will be removed/forgotten:
    if let Some(existing_endpoint) = existing_endpoint {
        // The following services will be removed/forgotten:
        let services_removed = existing_endpoint
            .services
            .iter()
            .filter(|svc| !discovered_service_names.contains(&svc.name))
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

    confirm_or_exit(&env, "Are you sure you want to apply those changes?")?;

    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(format!(
        "Asking restate server {} to apply endpoint discovery of {}",
        env.meta_base_url, discover_opts.endpoint
    ));

    let dry_run_result = client
        .discover_endpoint(mk_request_body(
            /* force = */ discover_opts.force,
            /* dry_run = */ false,
        ))
        .await?
        .into_body()
        .await?;

    progress.finish_and_clear();
    // print the result of the discovery
    c_success!("DISCOVERY RESULT:");
    let mut table = Table::new_styled(&env.ui_config);
    table.set_styled_header(vec!["SERVICE", "REV"]);
    for svc in dry_run_result.services {
        table.add_row(vec![svc.name, svc.revision.to_string()]);
    }
    c_println!("{}", table);

    Ok(())
}

async fn resolve_endpoint(
    client: &MetaClient,
    cache: &mut HashMap<String, ServiceEndpoint>,
    endpoint_id: &str,
) -> Option<ServiceEndpoint> {
    if cache.contains_key(endpoint_id) {
        return cache.get(endpoint_id).cloned();
    }

    let progress = ProgressBar::new_spinner();
    progress
        .set_style(indicatif::ProgressStyle::with_template("{spinner} [{elapsed}] {msg}").unwrap());
    progress.enable_steady_tick(std::time::Duration::from_millis(120));

    progress.set_message(format!(
        "Fetching information about existing endpoint {}",
        endpoint_id,
    ));

    let endpoint = client
        .get_endpoint(endpoint_id)
        .await
        .ok()?
        .into_body()
        .await
        .ok()
        .map(|endpoint| {
            let service_endpoint = endpoint.service_endpoint;
            cache.insert(endpoint_id.to_string(), service_endpoint.clone());
            Some(service_endpoint)
        })?;
    progress.finish_and_clear();

    endpoint
}
