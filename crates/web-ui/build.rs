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
use std::fs::DirEntry;
use std::path::{Path, PathBuf};

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap()).join("assets");

    let ui_zip = std::fs::read_dir("./assets")
        .expect("assets directory is there")
        .collect::<Result<Vec<DirEntry>, _>>()
        .expect("can read the assets directory")
        .into_iter()
        .find(|entry| {
            entry.path().to_string_lossy().contains("ui-")
                && entry.path().to_string_lossy().contains(".zip")
        })
        .expect("a ui-.zip must be present");

    unzip(&ui_zip.path(), &out_dir)
}

fn unzip(zip_out_file: &Path, out_name: &Path) {
    let zip_out_file_copy = zip_out_file.to_owned();
    let out_dir_copy = out_name.to_owned();

    let zip_file = std::fs::File::open(zip_out_file_copy).expect("the zip file should exist");
    let mut archive = zip::ZipArchive::new(zip_file).expect("opening the zip should work");
    archive
        .extract(out_dir_copy)
        .expect("extracting the zip should work");
}
