// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::{self, File};
use std::path::PathBuf;

use anyhow::{Context, bail};

/// Unzips the single `assets/ui-*.zip` into `$OUT_DIR/assets`.
fn main() -> anyhow::Result<()> {
    println!("cargo::rerun-if-changed=assets");

    let mut zips = fs::read_dir("assets")
        .context("reading the assets directory")?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("ui-") && name.ends_with(".zip"))
        })
        .collect::<Vec<_>>();
    let ui_zip = match zips.len() {
        1 => zips.pop().unwrap(),
        n => bail!("expected exactly one assets/ui-*.zip, found {n}"),
    };

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?).join("assets");
    if out_dir.exists() {
        fs::remove_dir_all(&out_dir)?;
    }
    fs::create_dir_all(&out_dir)?;
    zip::ZipArchive::new(File::open(&ui_zip)?)?
        .extract(&out_dir)
        .with_context(|| format!("unzipping {}", ui_zip.display()))
}
