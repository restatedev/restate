use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::cluster::cluster_state::NodeState;
use crate::PlainNodeId;

use super::{define_rpc, TargetName};

pub type NodesSnapshot = BTreeMap<PlainNodeId, NodeState>;

/// Gossip Push message. Is pushed from each node to every other known node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipRequest {
    pub payload: GossipPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipResponse {
    pub payload: GossipPayload,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipPayload {
    /// Snapshot is a copy of the sender cluster state
    /// This can include all nodes or a subset of nodes
    Snapshot {
        #[serde_as(as = "serde_with::Seq<(DisplayFromStr, _)>")]
        nodes: NodesSnapshot,
    },
}

define_rpc! {
    @request= GossipRequest,
    @response= GossipResponse,
    @request_target=TargetName::NodeGossipRequest,
    @response_target=TargetName::NodeGossipResponse,
}
