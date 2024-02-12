// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_pb::restate::common;

pub mod node_ctrl {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]

    tonic::include_proto!("dev.restate.node_ctrl");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("node_ctrl_descriptor");
}

pub mod cluster_controller {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]

    tonic::include_proto!("dev.restate.cluster_controller");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("cluster_controller_descriptor");
}
