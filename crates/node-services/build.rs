// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("cluster_ctrl_svc_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".dev.restate.common", "::restate_node_protocol::common")
        .compile(
            &["./proto/cluster_ctrl_svc.proto"],
            &["proto", "../node-protocol/proto"],
        )?;

    tonic_build::configure()
        .bytes(["."])
        .file_descriptor_set_path(out_dir.join("node_svc_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".dev.restate.node", "::restate_node_protocol::node")
        .extern_path(".dev.restate.common", "::restate_node_protocol::common")
        .compile(
            &["./proto/node_svc.proto"],
            &["proto", "../node-protocol/proto"],
        )?;

    Ok(())
}
