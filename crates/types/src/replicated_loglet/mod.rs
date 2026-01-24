// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod log_nodeset;
mod params;
mod spread;

pub use log_nodeset::*;
pub use params::*;
pub use spread::*;

use crate::PlainNodeId;
use crate::nodes_config::{NodeConfig, StorageState};

pub fn logserver_candidate_filter(_node_id: PlainNodeId, config: &NodeConfig) -> bool {
    // Important note: we check if the server has role=log-server when storage_state is
    // provisioning because all nodes get provisioning storage by default, we only care about
    // log-servers so we avoid adding other nodes in the nodeset. In the case of read-write, we
    // don't check the role to not accidentally consider those nodes as non-logservers even if
    // the role was removed by mistake (although some protection should be added for this)
    match config.log_server_config.storage_state {
        StorageState::ReadWrite => true,
        // Why is this being commented out?
        // Just being conservative to avoid polluting nodesets with nodes that might have started
        // and crashed and never became log-servers. If enough nodes in this state were added to
        // nodesets, we might never be able to seal those loglets unless we actually start those
        // nodes.
        // The origin of allowing those nodes to be in new nodesets came from the need to
        // swap log-servers with new ones on rolling upgrades (N5 starts up to replace N1 for instance).
        // This requires that we drain N1 (marking it read-only) and start up N5. New nodesets
        // after this point should not include N1 and will include N5. For now, I prefer to
        // constraint this until we have the full story on how those operations will be
        // coordinated.
        //
        // StorageState::Provisioning if config.has_role(Role::LogServer) => true,
        // explicit match to make it clear that we are excluding nodes with the following states,
        // any new states added will force the compiler to fail
        StorageState::Provisioning
        | StorageState::Disabled
        | StorageState::ReadOnly
        | StorageState::Gone
        | StorageState::DataLoss => false,
    }
}
