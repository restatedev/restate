// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU32, NonZeroU8};

use anyhow::Context;
use cling::prelude::*;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use restate_admin::cluster_controller::protobuf::cluster_ctrl_svc_client::ClusterCtrlSvcClient;
use restate_admin::cluster_controller::protobuf::{
    ChainExtension, ListLogsRequest, SealAndExtendChainRequest,
};
use restate_cli_util::{c_eprintln, c_println};
use restate_types::logs::metadata::{Logs, ProviderKind, SegmentIndex};
use restate_types::logs::{LogId, LogletId};
use restate_types::protobuf::common::Version;
use restate_types::replicated_loglet::{NodeSet, ReplicatedLogletParams, ReplicationProperty};
use restate_types::storage::StorageCodec;
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::app::ConnectionInfo;
use crate::util::grpc_connect;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "reconfigure")]
pub struct ReconfigureOpts {
    /// LogId/Partition to seal and extend
    #[clap(long, short)]
    log_id: u32,
    /// If a loglet provider is specified, the new loglet will be configured using the provided parameters.
    /// Otherwise, it will be automatically reconfigured based on the cluster configuration.
    #[clap(long, short)]
    provider: Option<ProviderKind>,
    /// Option segment index to seal. The tail segment is chosen automatically if not provided.
    #[clap(long, short)]
    segment_index: Option<u32>,
    /// The [minimum] expected metadata version
    #[clap(long, short, default_value = "1")]
    min_version: NonZeroU32,
    /// Replication factor requirement for a new replicated segment; by default reuse
    /// the sealed segment's value iff its provider kind was also "replicated"
    #[clap(long)]
    replication_factor_nodes: Option<NonZeroU8>,
    /// (not implemented)
    /// Replication requirement for a new replicated segment; by default reuse
    /// the sealed segment's value iff its provider kind was also "replicated"
    #[clap(hide = true, conflicts_with = "replication_factor_nodes")]
    replication: Option<String>,
    /// A comma-separated list of the nodes in the nodeset. e.g. N1,N2,N4 or 1,2,3
    /// By default reuse the sealed segment's value iff its provider kind was also "replicated"
    #[clap(long, value_delimiter=',', num_args = 0..)]
    nodeset: Vec<PlainNodeId>,
    /// The generational node id of the sequencer node, e.g. N1:1
    /// By default reuse the sealed segment's value iff its provider kind was also "replicated"
    #[clap(long)]
    sequencer: Option<GenerationalNodeId>,
}

async fn reconfigure(connection: &ConnectionInfo, opts: &ReconfigureOpts) -> anyhow::Result<()> {
    let channel = grpc_connect(connection.cluster_controller.clone())
        .await
        .with_context(|| {
            format!(
                "cannot connect to cluster controller at {}",
                connection.cluster_controller
            )
        })?;

    let mut client =
        ClusterCtrlSvcClient::new(channel).accept_compressed(CompressionEncoding::Gzip);

    let extension = match opts.provider {
        Some(provider) => {
            let params = match provider {
                ProviderKind::Local => rand::random::<u64>().to_string(),
                #[cfg(any(test, feature = "memory-loglet"))]
                ProviderKind::InMemory => rand::random::<u64>().to_string(),
                #[cfg(feature = "replicated-loglet")]
                ProviderKind::Replicated => replicated_loglet_params(&mut client, opts).await?,
            };

            Some(ChainExtension {
                provider: provider.to_string(),
                segment_index: opts.segment_index,
                params,
            })
        }
        None => None,
    };

    let response = client
        .seal_and_extend_chain(SealAndExtendChainRequest {
            log_id: opts.log_id,
            min_version: Some(Version {
                value: opts.min_version.get(),
            }),
            extension,
        })
        .await?
        .into_inner();

    let Some(sealed_segment) = response.sealed_segment else {
        c_println!("✅ Log scheduled for reconfiguration");
        return Ok(());
    };

    c_println!("✅ Log reconfiguration");
    c_println!(" └ Segment Index: {}", response.new_segment_index);

    c_println!();
    c_println!("Sealed segment");
    c_println!(" ├ Index: {}", response.new_segment_index - 1);
    c_println!(" ├ Tail LSN: {}", sealed_segment.tail_offset);
    c_println!(" ├ Provider: {}", sealed_segment.provider);
    let Ok(provider) = ProviderKind::from_str(&sealed_segment.provider, true) else {
        c_eprintln!("Unkown provider type '{}'", sealed_segment.provider);
        return Ok(());
    };

    if provider == ProviderKind::Replicated {
        let params = ReplicatedLogletParams::deserialize_from(sealed_segment.params.as_bytes())?;
        c_println!(" ├ Loglet ID: {}", params.loglet_id);
        c_println!(" ├ Sequencer: {}", params.sequencer);
        c_println!(" ├ Replication: {}", params.replication);
        c_println!(" └ Node Set: {:#}", params.nodeset);
    } else {
        c_println!(" └ Params: {}", sealed_segment.params);
    }

    Ok(())
}

async fn replicated_loglet_params(
    client: &mut ClusterCtrlSvcClient<Channel>,
    opts: &ReconfigureOpts,
) -> anyhow::Result<String> {
    let mut logs_response = client
        .list_logs(ListLogsRequest {})
        .await
        .context("Failed to get logs metadata")?
        .into_inner();

    let logs = StorageCodec::decode::<Logs, _>(&mut logs_response.logs)?;
    let log_id = LogId::from(opts.log_id);
    let chain = logs
        .chain(&log_id)
        .with_context(|| format!("Unknown log id '{log_id}'"))?;

    let tail_index = opts
        .segment_index
        .map(SegmentIndex::from)
        .unwrap_or(chain.tail_index());

    let loglet_id = LogletId::new(log_id, tail_index.next());

    let tail_segment = chain.tail();

    let params = if tail_segment.config.kind == ProviderKind::Replicated {
        let last_params =
            ReplicatedLogletParams::deserialize_from(tail_segment.config.params.as_bytes())
                .context("Last segment params in chain is invalid")?;

        ReplicatedLogletParams {
            loglet_id,
            nodeset: if opts.nodeset.is_empty() {
                last_params.nodeset
            } else {
                NodeSet::from_iter(opts.nodeset.iter().cloned())
            },
            replication: opts
                .replication_factor_nodes
                .map(ReplicationProperty::new)
                .unwrap_or(last_params.replication.clone()),
            sequencer: opts.sequencer.unwrap_or(last_params.sequencer),
        }
    } else {
        ReplicatedLogletParams {
            loglet_id,
            nodeset: if opts.nodeset.is_empty() {
                anyhow::bail!("Missing nodeset. Nodeset is required if last segment is not of replicated type");
            } else {
                NodeSet::from_iter(opts.nodeset.iter().cloned())
            },
            replication: ReplicationProperty::new(
                opts.replication_factor_nodes.context("Missing replication-factor. Replication factor is required if last segment is not of replicated type")?,
            ),
            sequencer: opts.sequencer.context("Missing sequencer. Sequencer is required if last segment is not of replicated type")?,
        }
    };

    params.serialize().map_err(Into::into)
}
