// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use jsonptr::Pointer;
use std::env;
use std::fs::File;
use std::path::{Path, PathBuf};
use typify::{TypeSpace, TypeSpaceSettings};

fn main() -> std::io::Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // Old service-protocol
    prost_build::Config::new()
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .enum_attribute(
            "protocol.ServiceProtocolVersion",
            "#[derive(::serde::Serialize, ::serde::Deserialize)]",
        )
        .compile_protos(
            &["service-protocol-v3/dev/restate/service/protocol.proto"],
            &["service-protocol-v3"],
        )?;

    // Version enums!
    // TODO when removing the old service-protocol, include here the service-protocol enum too
    prost_build::Config::new()
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .enum_attribute(
            "discovery.ServiceDiscoveryProtocolVersion",
            "#[derive(::strum::EnumIter)]",
        )
        .compile_protos(
            &[
                Path::new("../../service-protocol/dev/restate/service/discovery.proto")
                    .canonicalize()
                    .unwrap(),
            ],
            &[Path::new("../../service-protocol").canonicalize().unwrap()],
        )?;

    // Common proto types for internal use
    build_restate_proto(&out_dir)?;

    let mut parsed_content: serde_json::Value = serde_json::from_reader(
        File::open("../../service-protocol/endpoint_manifest_schema.json").unwrap(),
    )
    .unwrap();

    // Patch schema for https://github.com/oxidecomputer/typify/issues/531
    // We can get rid of this once the issue in typify is solved.
    Pointer::parse("/properties/services/items/properties/handlers/items/properties/input/default")
        .unwrap()
        .delete(&mut parsed_content);
    Pointer::parse(
        "/properties/services/items/properties/handlers/items/properties/input/examples",
    )
    .unwrap()
    .delete(&mut parsed_content);
    Pointer::parse(
        "/properties/services/items/properties/handlers/items/properties/output/default",
    )
    .unwrap()
    .delete(&mut parsed_content);
    Pointer::parse(
        "/properties/services/items/properties/handlers/items/properties/output/examples",
    )
    .unwrap()
    .delete(&mut parsed_content);

    // Instantiate type space and run code-generation
    let mut type_space =
        TypeSpace::new(TypeSpaceSettings::default().with_derive("Clone".to_owned()));
    type_space
        .add_root_schema(serde_json::from_value(parsed_content).unwrap())
        .unwrap();

    let contents =
        prettyplease::unparse(&syn::parse2::<syn::File>(type_space.to_stream()).unwrap());

    std::fs::write(out_dir.join("endpoint_manifest.rs"), contents)?;

    // Generate short ID strings
    generate_short_id_strings(&out_dir)
}

/// Generates compile-time string representations for short IDs commonly used in metric labels.
fn generate_short_id_strings(out_dir: &Path) -> std::io::Result<()> {
    use std::fmt::Write;

    const MAX_PRECOMPUTED: u32 = 1025; // 0..=1024

    let mut code = String::new();

    // Plain numeric strings for PartitionId and LogId
    writeln!(
        code,
        "/// Pre-computed string representations for short IDs (0-1024)."
    )
    .unwrap();
    writeln!(code, "/// Generated at build time for zero-cost lookup.").unwrap();
    writeln!(
        code,
        "pub(crate) static SHORT_ID_STRINGS: [&str; {MAX_PRECOMPUTED}] = ["
    )
    .unwrap();
    for i in 0..MAX_PRECOMPUTED {
        writeln!(code, "    \"{i}\",").unwrap();
    }
    writeln!(code, "];").unwrap();
    writeln!(code).unwrap();

    // N-prefixed strings for PlainNodeId (Display format)
    writeln!(
        code,
        "/// Pre-computed N-prefixed strings for PlainNodeId labels (N0-N1024)."
    )
    .unwrap();
    writeln!(
        code,
        "pub(crate) static NODE_ID_STRINGS: [&str; {MAX_PRECOMPUTED}] = ["
    )
    .unwrap();
    for i in 0..MAX_PRECOMPUTED {
        writeln!(code, "    \"N{i}\",").unwrap();
    }
    writeln!(code, "];").unwrap();

    std::fs::write(out_dir.join("short_id_strings.rs"), code)
}

fn build_restate_proto(out_dir: &Path) -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .enum_attribute(
            "ServiceTag",
            "#[derive(::enum_map::Enum, ::derive_more::IsVariant, ::derive_more::Display)]",
        )
        .enum_attribute(
            "NodeStatus",
            "#[derive(::serde::Serialize, ::derive_more::IsVariant)]",
        )
        .enum_attribute("AdminStatus", "#[derive(::serde::Serialize)]")
        .enum_attribute("LogServerStatus", "#[derive(::serde::Serialize)]")
        .enum_attribute("WorkerStatus", "#[derive(::serde::Serialize)]")
        .enum_attribute("MetadataServerStatus", "#[derive(::serde::Serialize)]")
        .btree_map([
            ".restate.cluster.ClusterState",
            ".restate.cluster.AliveNode",
        ])
        .file_descriptor_set_path(out_dir.join("common_descriptor.bin"))
        // allow older protobuf compiler to be used
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &[
                "./protobuf/restate/common.proto",
                "./protobuf/restate/metadata.proto",
                "./protobuf/restate/cluster.proto",
                "./protobuf/restate/journal_events.proto",
                "./protobuf/restate/log_server_common.proto",
            ],
            &["protobuf"],
        )?;
    Ok(())
}
