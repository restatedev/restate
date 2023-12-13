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
use std::fs::read_dir;
use std::path::{Path, PathBuf};

// Walk recursively and create descriptors, combining all the .proto in a sub-directory
fn walk_and_create_descriptors(
    root: impl AsRef<Path>,
    this_dir: impl AsRef<Path>,
) -> std::io::Result<()> {
    let mut subdirs = vec![];
    let mut proto_files = vec![];

    for d in read_dir(this_dir.as_ref())? {
        let dir_entry = d?;
        let file_type = dir_entry.file_type()?;
        let path = dir_entry.path();
        if file_type.is_dir() {
            subdirs.push(path);
        } else if file_type.is_file() && path.extension().is_some_and(|s| s == "proto") {
            proto_files.push(path);
        }
    }

    if !proto_files.is_empty() {
        // Compute sub-dir
        let this_subdir = this_dir.as_ref().strip_prefix(root.as_ref()).unwrap();
        let rust_out_dir =
            PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
                .join(this_subdir);
        std::fs::create_dir_all(rust_out_dir.clone())?;
        let out_descriptor = rust_out_dir.clone().join("descriptor.bin");

        println!("Compiling {:?} to {}", proto_files, rust_out_dir.display());

        // Compile this dir if any
        prost_build::Config::new()
            .out_dir(rust_out_dir)
            .file_descriptor_set_path(out_descriptor)
            .extern_path(".dev.restate", "crate::restate")
            .compile_protos(&proto_files, &[this_dir.as_ref()])?;
    }

    // Recursively check the other subdirs
    for subdir in subdirs {
        walk_and_create_descriptors(root.as_ref(), subdir)?;
    }

    Ok(())
}

fn main() -> std::io::Result<()> {
    let proto_dir = std::fs::canonicalize("tests")?;

    walk_and_create_descriptors(proto_dir.clone(), proto_dir.join("pb"))?;

    Ok(())
}
