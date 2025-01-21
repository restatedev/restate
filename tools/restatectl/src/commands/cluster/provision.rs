// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::app::ConnectionInfo;
use crate::commands::cluster::config::cluster_config_string;
use crate::util::grpc_channel;
use clap::Parser;
use cling::{Collect, Run};
use restate_cli_util::ui::console::confirm_or_exit;
use restate_cli_util::{c_error, c_println, c_warn};
use restate_core::protobuf::node_ctl_svc::node_ctl_svc_client::NodeCtlSvcClient;
use restate_core::protobuf::node_ctl_svc::ProvisionClusterRequest;
use restate_types::logs::metadata::{ProviderConfiguration, ProviderKind, ReplicatedLogletConfig};
use restate_types::net::AdvertisedAddress;
use restate_types::partition_table::ReplicationStrategy;
use restate_types::replicated_loglet::ReplicationProperty;
use std::num::NonZeroU16;
use tonic::codec::CompressionEncoding;
use tonic::Code;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "cluster_provision")]
pub struct ProvisionOpts {
    /// Address of the node that should be provisioned
    #[clap(long)]
    address: Option<AdvertisedAddress>,

    /// Number of partitions
    #[clap(long)]
    num_partitions: Option<NonZeroU16>,

    /// Replication strategy. Possible values
    /// are `on-all-nodes` or `factor(n)`
    #[clap(long)]
    replication_strategy: Option<ReplicationStrategy>,

    /// Default log provider kind
    #[clap(long)]
    bifrost_provider: Option<ProviderKind>,

    /// Replication property
    #[clap(long, required_if_eq("bifrost_provider", "replicated"))]
    replication_property: Option<ReplicationProperty>,
}

async fn cluster_provision(
    connection_info: &ConnectionInfo,
    provision_opts: &ProvisionOpts,
) -> anyhow::Result<()> {
    let node_address = provision_opts
        .address
        .clone()
        .unwrap_or_else(|| connection_info.cluster_controller.clone());
    let channel = grpc_channel(node_address.clone());

    let mut client = NodeCtlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let log_provider = provision_opts.bifrost_provider.map(|bifrost_provider| {
        extract_default_provider(
            bifrost_provider,
            provision_opts.replication_property.clone(),
        )
    });

    let request = ProvisionClusterRequest {
        dry_run: true,
        num_partitions: provision_opts.num_partitions.map(|n| u32::from(n.get())),
        placement_strategy: provision_opts.replication_strategy.map(Into::into),
        log_provider: log_provider.map(Into::into),
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

    if let Some(default_provider) = &cluster_configuration_to_provision.default_provider {
        let default_provider = ProviderConfiguration::try_from(default_provider.clone())?;

        match default_provider {
            ProviderConfiguration::InMemory | ProviderConfiguration::Local => {
                c_warn!("You are about to provision a cluster with a Bifrost provider that only supports a single node cluster.");
            }
            ProviderConfiguration::Replicated(_) => {
                // nothing to do
            }
        }
    }

    confirm_or_exit("Provision cluster with this configuration?")?;

    let request = ProvisionClusterRequest {
        dry_run: false,
        num_partitions: Some(cluster_configuration_to_provision.num_partitions),
        placement_strategy: cluster_configuration_to_provision.replication_strategy,
        log_provider: cluster_configuration_to_provision.default_provider,
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

pub fn extract_default_provider(
    bifrost_provider: ProviderKind,
    replication_property: Option<ReplicationProperty>,
) -> ProviderConfiguration {
    match bifrost_provider {
        ProviderKind::InMemory => ProviderConfiguration::InMemory,
        ProviderKind::Local => ProviderConfiguration::Local,
        ProviderKind::Replicated => {
            let config = ReplicatedLogletConfig {
                replication_property: replication_property.clone().expect("is required"),
            };
            ProviderConfiguration::Replicated(config)
        }
    }
}
