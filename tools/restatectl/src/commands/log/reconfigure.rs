// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use restate_admin::cluster_controller::protobuf::{ListLogsRequest, SealAndExtendChainRequest};
use restate_types::logs::metadata::{Logs, ProviderKind, SegmentIndex};
use restate_types::logs::LogId;
use restate_types::protobuf::common::Version;
use restate_types::replicated_loglet::{
    NodeSet, ReplicatedLogletId, ReplicatedLogletParams, ReplicationProperty,
};
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
    /// Option segment index to seal. The tail segment is chosen automatically if not provided.
    #[clap(long, short)]
    segment_index: Option<u32>,
    /// The [minimum] expected metadata version
    #[clap(long, short, default_value = "1")]
    min_version: NonZeroU32,
    /// Provider kind.
    #[clap(long, short, default_value = "replicated")]
    provider: ProviderKind,

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

    let params = match opts.provider {
        ProviderKind::Local => rand::random::<u64>().to_string(),
        #[cfg(any(test, feature = "memory-loglet"))]
        ProviderKind::InMemory => rand::random::<u64>().to_string(),
        #[cfg(feature = "replicated-loglet")]
        ProviderKind::Replicated => replicated_loglet_params(&mut client, opts).await?,
    };

    client
        .seal_and_extend_chain(SealAndExtendChainRequest {
            log_id: opts.log_id,
            min_version: Some(Version {
                value: opts.min_version.get(),
            }),
            provider: opts.provider.to_string(),
            segment_index: opts.segment_index,
            params,
        })
        .await?;

    Ok(())
}

async fn replicated_loglet_params(
    client: &mut ClusterCtrlSvcClient<Channel>,
    opts: &ReconfigureOpts,
) -> anyhow::Result<String> {
    let mut logs_resposne = client
        .list_logs(ListLogsRequest {})
        .await
        .context("Failed to get logs metadata")?
        .into_inner();

    let logs = StorageCodec::decode::<Logs, _>(&mut logs_resposne.logs)?;
    let log_id = LogId::from(opts.log_id);
    let chain = logs
        .chain(&log_id)
        .with_context(|| format!("Unknown log id '{}'", log_id))?;

    let tail_index = opts
        .segment_index
        .map(SegmentIndex::from)
        .unwrap_or(chain.tail_index());

    let loglet_id = ReplicatedLogletId::new(log_id, tail_index.next());

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
            write_set: None,
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
            write_set: None,
        }
    };

    params.serialize().map_err(Into::into)
}
