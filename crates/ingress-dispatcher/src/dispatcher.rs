// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{
    error::IngressDispatchError, IngressDispatcherRequest, IngressDispatcherRequestInner,
    IngressRequestMode,
};

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tracing::debug;

use restate_bifrost::Bifrost;
use restate_core::metadata;
use restate_storage_api::deduplication_table::DedupInformation;
use restate_types::identifiers::{PartitionKey, WithPartitionKey};
use restate_types::message::MessageIndex;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::{
    append_envelope_to_bifrost, Command, Destination, Envelope, Header, Source,
};

/// Dispatches a request from ingress to bifrost
pub trait DispatchIngressRequest {
    fn dispatch_ingress_request(
        &self,
        ingress_request: IngressDispatcherRequest,
    ) -> impl std::future::Future<Output = Result<(), IngressDispatchError>> + Send;
}

#[derive(Default)]
struct IngressDispatcherState {
    msg_index: AtomicU64,
}

impl IngressDispatcherState {
    fn get_and_increment_msg_index(&self) -> MessageIndex {
        self.msg_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct IngressDispatcher {
    bifrost: Bifrost,
    state: Arc<IngressDispatcherState>,
}
impl IngressDispatcher {
    pub fn new(bifrost: Bifrost) -> Self {
        Self {
            bifrost,
            state: Arc::new(IngressDispatcherState::default()),
        }
    }
}

impl DispatchIngressRequest for IngressDispatcher {
    async fn dispatch_ingress_request(
        &self,
        ingress_request: IngressDispatcherRequest,
    ) -> Result<(), IngressDispatchError> {
        let IngressDispatcherRequest {
            inner,
            request_mode,
        } = ingress_request;

        let (dedup_source, msg_index, proxying_partition_key) = match request_mode {
            IngressRequestMode::FireAndForget => {
                let msg_index = self.state.get_and_increment_msg_index();
                (None, msg_index, None)
            }
            IngressRequestMode::DedupFireAndForget {
                deduplication_id,
                proxying_partition_key,
            } => (
                Some(deduplication_id.0),
                deduplication_id.1,
                proxying_partition_key,
            ),
        };

        let partition_key = proxying_partition_key.unwrap_or_else(|| inner.partition_key());

        let envelope = wrap_service_invocation_in_envelope(
            partition_key,
            inner,
            metadata().my_node_id(),
            dedup_source,
            msg_index,
        );
        let (log_id, lsn) = append_envelope_to_bifrost(&self.bifrost, Arc::new(envelope)).await?;

        debug!(
            log_id = %log_id,
            lsn = %lsn,
            "Ingress request written to bifrost"
        );
        Ok(())
    }
}

fn wrap_service_invocation_in_envelope(
    partition_key: PartitionKey,
    inner: IngressDispatcherRequestInner,
    from_node_id: GenerationalNodeId,
    deduplication_source: Option<String>,
    msg_index: MessageIndex,
) -> Envelope {
    let header = Header {
        source: Source::Ingress {
            node_id: from_node_id,
            nodes_config_version: metadata().nodes_config_version(),
        },
        dest: Destination::Processor {
            partition_key,
            dedup: deduplication_source.map(|src| DedupInformation::ingress(src, msg_index)),
        },
    };

    Envelope::new(
        header,
        match inner {
            IngressDispatcherRequestInner::Invoke(si) => Command::Invoke(si),
            IngressDispatcherRequestInner::ProxyThrough(si) => Command::ProxyThrough(si),
        },
    )
}
