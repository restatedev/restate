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
use std::fs::{DirEntry, File, create_dir_all};
use std::path::{Path, PathBuf};

use anyhow::Context;

fn main() -> anyhow::Result<()> {
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

fn unzip(zip_out_file: &Path, out_name: &Path) -> anyhow::Result<()> {
    let zip_out_file_copy = zip_out_file.to_owned();
    let out_dir_copy = out_name.to_owned();

    let zip_file = File::open(zip_out_file_copy)?;
    let mut archive = zip::ZipArchive::new(zip_file)?;
    create_dir_all(&out_dir_copy)?;
    archive.extract(out_dir_copy).context("unzip failed")
}
