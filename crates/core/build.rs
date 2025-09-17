// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

    tonic_prost_build::configure()
        .bytes(".")
        .file_descriptor_set_path(out_dir.join("cluster_ctrl_svc_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".restate.common", "::restate_types::protobuf::common")
        .extern_path(".restate.cluster", "::restate_types::protobuf::cluster")
        .compile_protos(
            &["./protobuf/cluster_ctrl_svc.proto"],
            &["protobuf", "../types/protobuf"],
        )?;

    tonic_prost_build::configure()
        .bytes(".")
        .file_descriptor_set_path(out_dir.join("node_ctl_svc_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".restate.common", "::restate_types::protobuf::common")
        .extern_path(".restate.cluster", "::restate_types::protobuf::cluster")
        .compile_protos(
            &["./protobuf/node_ctl_svc.proto"],
            &["protobuf", "../types/protobuf"],
        )?;

    tonic_prost_build::configure()
        .bytes(".")
        .enum_attribute("Datagram", "#[derive(::derive_more::From)]")
        .enum_attribute("Datagram.datagram", "#[derive(::derive_more::From)]")
        .enum_attribute(
            "Message.body",
            "#[derive(::derive_more::IsVariant, ::derive_more::From)]",
        )
        .enum_attribute("Body", "#[derive(::derive_more::From)]")
        .file_descriptor_set_path(out_dir.join("core_node_svc_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".restate.common", "::restate_types::protobuf::common")
        .extern_path(".restate.node", "::restate_types::protobuf::node")
        .compile_protos(
            &[
                "./protobuf/restate/network.proto",
                "./protobuf/core_node_svc.proto",
            ],
            &["protobuf", "../types/protobuf"],
        )?;

    Ok(())
}
