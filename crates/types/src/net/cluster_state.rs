use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, FromInto};

use crate::{
    cluster::cluster_state::{AliveNode, PartitionProcessorStatus},
    identifiers::PartitionId,
    time::MillisSinceEpoch,
    GenerationalNodeId, PlainNodeId,
};

use super::{define_rpc, TargetName};

define_rpc! {
    @request= ClusterStateRequest,
    @response= ClusterStateResponse,
    @request_target=TargetName::NodeClusterStateRequest,
    @response_target=TargetName::NodeClusterStateResponse,
}

#[serde_as]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeData {
    pub last_update_time: MillisSinceEpoch,
    pub generational_node_id: GenerationalNodeId,
    #[serde_as(as = "serde_with::Seq<(FromInto<u16>, _)>")]
    pub partitions: BTreeMap<PartitionId, PartitionProcessorStatus>,
}

impl From<NodeData> for AliveNode {
    fn from(value: NodeData) -> Self {
        Self {
            generational_node_id: value.generational_node_id,
            last_heartbeat_at: value.last_update_time,
            partitions: value.partitions,
        }
    }
}

impl Hash for NodeData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.generational_node_id.hash(state);
        self.partitions.hash(state);
    }
}

impl NodeData {
    pub fn hash_value(&self) -> u64 {
        let mut hasher = std::hash::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub fn as_node_hash(&self) -> NodeHash {
        NodeHash {
            hash_value: self.hash_value(),
            last_update_time: self.last_update_time,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeHash {
    pub last_update_time: MillisSinceEpoch,
    pub hash_value: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, strum::EnumIs)]
pub enum NodeRecord {
    Data(NodeData),
    Hash(NodeHash),
}

impl NodeRecord {
    pub fn last_update_time(&self) -> MillisSinceEpoch {
        match self {
            Self::Hash(h) => h.last_update_time,
            Self::Data(s) => s.last_update_time,
        }
    }

    pub fn hash_value(&self) -> u64 {
        match self {
            Self::Hash(h) => h.hash_value,
            Self::Data(s) => s.hash_value(),
        }
    }
}

impl From<NodeData> for NodeRecord {
    fn from(value: NodeData) -> Self {
        Self::Data(value)
    }
}

impl From<NodeHash> for NodeRecord {
    fn from(value: NodeHash) -> Self {
        Self::Hash(value)
    }
}

/// Gossip Push message. Is pushed from each node to every other known node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateRequest {
    pub payload: GossipPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStateResponse {
    pub payload: GossipPayload,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipPayload {
    pub record: NodeRecord,
    #[serde_as(as = "serde_with::Seq<(FromInto<u32>, _)>")]
    pub cluster: HashMap<PlainNodeId, NodeRecord>,
}

#[cfg(test)]
mod test {
    use crate::{
        cluster::cluster_state::PartitionProcessorStatus,
        identifiers::PartitionId,
        net::cluster_state::{NodeHash, NodeRecord},
        time::MillisSinceEpoch,
        GenerationalNodeId, PlainNodeId,
    };

    use super::{GossipPayload, NodeData};

    #[test]
    fn encoding() {
        // flexbuffers is tricky with map types
        // this test is to make sure changes to the types does not
        // break the encoding
        let payload = GossipPayload {
            record: NodeRecord::Data(NodeData {
                generational_node_id: GenerationalNodeId::new(1, 1),
                last_update_time: MillisSinceEpoch::now(),
                partitions: vec![(PartitionId::from(1), PartitionProcessorStatus::default())]
                    .into_iter()
                    .collect(),
            }),
            cluster: vec![(
                PlainNodeId::new(10),
                NodeRecord::Hash(NodeHash {
                    hash_value: 10,
                    last_update_time: MillisSinceEpoch::now(),
                }),
            )]
            .into_iter()
            .collect(),
        };

        let result = flexbuffers::to_vec(&payload);
        assert!(result.is_ok());

        let loaded: Result<GossipPayload, _> = flexbuffers::from_slice(&result.unwrap());
        assert!(loaded.is_ok());
        let loaded = loaded.unwrap();

        assert_eq!(payload.record, loaded.record);
    }
}
