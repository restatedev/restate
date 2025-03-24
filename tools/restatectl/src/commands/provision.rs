// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::commands::config::cluster_config_string;
use crate::connection::ConnectionInfo;
use crate::util::grpc_channel;
use clap::Parser;
use cling::{Collect, Run};
use restate_cli_util::ui::console::confirm_or_exit;
use restate_cli_util::{c_error, c_println, c_warn};
use restate_core::protobuf::node_ctl_svc::ProvisionClusterRequest;
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_types::logs::metadata::{ProviderConfiguration, ProviderKind};
use restate_types::replication::ReplicationProperty;
use std::cmp::Ordering;
use tonic::Code;
use tonic::codec::CompressionEncoding;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "provision_cluster")]
pub struct ProvisionOpts {
    /// Number of partitions
    #[clap(long)]
    num_partitions: Option<u16>,

    /// Optional partition placement strategy. By default replicates
    /// partitions on all nodes. Accepts replication property
    /// string as a value
    #[clap(long)]
    partition_replication: Option<ReplicationProperty>,

    /// Default log provider kind
    #[clap(long)]
    log_provider: Option<ProviderKind>,

    /// Replication property of bifrost logs if using replicated as log provider
    #[clap(long)]
    log_replication: Option<ReplicationProperty>,

    /// The nodeset size used for replicated log, this is an advanced feature.
    /// It's recommended to leave it unset (defaults to 0)
    #[clap(long)]
    log_default_nodeset_size: Option<u16>,
}

async fn provision_cluster(
    connection: &ConnectionInfo,
    provision_opts: &ProvisionOpts,
) -> anyhow::Result<()> {
    let address = match connection.address.len().cmp(&1) {
        Ordering::Greater => {
            let address = &connection.address[0];
            c_println!(
                "Cluster provisioning must be performed on a single node. Using {address} for provisioning.",
            );
            address
        }
        Ordering::Equal => &connection.address[0],
        Ordering::Less => {
            anyhow::bail!("At least one address must be specified to provision");
        }
    };

    let channel = grpc_channel(address.clone());

    let mut client = NodeCtlSvcClient::new(channel)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Gzip);

    let request = ProvisionClusterRequest {
        dry_run: true,
        num_partitions: provision_opts.num_partitions.map(u32::from),
        partition_replication: provision_opts.partition_replication.clone().map(Into::into),
        log_provider: provision_opts
            .log_provider
            .map(|provider| provider.to_string()),
        log_replication: provision_opts.log_replication.clone().map(Into::into),
        target_nodeset_size: provision_opts.log_default_nodeset_size.map(Into::into),
    };

    let response = match client.provision_cluster(request).await {
        Ok(response) => response.into_inner(),
        Err(err) => {
            c_error!(
                "Failed to provision cluster during dry run: {}",
                err.message()
            );
            return Ok(());
        }
    };

    debug_assert!(response.dry_run, "Provision with dry run");
    let cluster_configuration_to_provision = response
        .cluster_configuration
        .expect("Provision response should carry a cluster configuration");

    c_println!(
        "{}",
        cluster_config_string(&cluster_configuration_to_provision)?
    );

    if let Some(default_provider) = &cluster_configuration_to_provision.bifrost_provider {
        let default_provider = ProviderConfiguration::try_from(default_provider.clone())?;

        match default_provider {
            ProviderConfiguration::InMemory | ProviderConfiguration::Local => {
                c_warn!(
                    "You are about to provision a cluster with a Bifrost provider that only supports a single node cluster."
                );
            }
            ProviderConfiguration::Replicated(_) => {
                // nothing to do
            }
        }
    }

    confirm_or_exit("Provision cluster with this configuration?")?;

    let (num_partitions, partition_replication, bifrost_provider) =
        cluster_configuration_to_provision.into_inner();
    let (log_provider, log_replication, target_nodeset_size) =
        bifrost_provider.map_or((None, None, None), |bifrost| {
            let (log_provider, log_replication, target_nodeset_size) = bifrost.into_inner();
            (
                Some(log_provider),
                log_replication,
                Some(target_nodeset_size),
            )
        });

    let request = ProvisionClusterRequest {
        dry_run: false,
        num_partitions: Some(num_partitions),
        partition_replication,
        log_provider,
        log_replication,
        target_nodeset_size,
    };

    match client.provision_cluster(request).await {
        Ok(response) => {
            let response = response.into_inner();
            debug_assert!(!response.dry_run, "Provision w/o dry run");

            c_println!("✅ Cluster has been successfully provisioned.");
        }
        Err(err) => {
            if err.code() == Code::AlreadyExists {
                c_println!("🤷 Cluster has been provisioned by somebody else.");
            } else {
                c_error!("Failed to provision cluster: {}", err.message());
            }
        }
    };

    Ok(())
}
