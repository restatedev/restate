// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::console::c_println;
use anyhow::{anyhow, bail, Context, Result};
use cling::prelude::*;
use convert_case::{Case, Casing};
use dialoguer::Select;
use futures::StreamExt;
use octocrab::models::repos::Asset;
use octocrab::repos::RepoHandler;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_examples")]
pub struct Examples {
    /// Example name
    name: Option<String>,
}

pub async fn run_examples(example_opts: &Examples) -> Result<()> {
    // Get latest release of the examples repo
    let octocrab = octocrab::instance();
    let examples_repo = octocrab.repos("restatedev", "examples");
    let latest_release = examples_repo
        .releases()
        .get_latest()
        .await
        .context("Can't access the examples releases. Please retry later")?;

    let example_asset = if let Some(example) = &example_opts.name {
        // Check if the example exists
        let example_lowercase = example.to_lowercase();
        latest_release
            .assets
            .into_iter()
            .find(|a| a.name.trim_end_matches(".zip") == example_lowercase)
            .ok_or(anyhow!(
                "Unknown example {}. Use `restate examples` to navigate the list of examples.",
                example_lowercase
            ))?
    } else {
        // Ask the example to download among the available examples
        let mut languages = parse_available_examples(latest_release.assets);
        let language_selection = Select::new()
            .with_prompt("Which language?")
            .items(&languages)
            .interact()
            .unwrap();
        let example_selection = Select::new()
            .with_prompt("Which example?")
            .items(&languages[language_selection].examples)
            .interact()
            .unwrap();

        languages
            .remove(language_selection)
            .examples
            .remove(example_selection)
            .asset
    };

    download_example(examples_repo, example_asset).await
}

struct Language {
    display_name: String,
    examples: Vec<Example>,
}

impl fmt::Display for Language {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

struct Example {
    display_name: String,
    asset: Asset,
}

impl fmt::Display for Example {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

fn parse_available_examples(assets: Vec<Asset>) -> Vec<Language> {
    let mut languages_map = HashMap::new();

    for asset in assets {
        // Zip names have the format [language]-[example_name].zip

        let asset_name = asset.name.clone();
        let mut split_asset_name = asset_name.splitn(2, '-');

        // First part is language
        let language = if let Some(lang) = split_asset_name.next() {
            lang.to_string()
        } else {
            // Bad name, just ignore it
            continue;
        };
        let language_display_name = capitalize(&language);

        // Second is example name
        let example_display_name = if let Some(example) = split_asset_name.next() {
            example
                .to_string()
                .from_case(Case::Kebab)
                .to_case(Case::Title)
        } else {
            // Bad name, just ignore it
            continue;
        };
        let example_display_name = example_display_name.trim_end_matches(".zip").to_string();

        languages_map
            .entry(language)
            .or_insert_with(|| Language {
                display_name: language_display_name,
                examples: vec![],
            })
            .examples
            .push(Example {
                display_name: example_display_name,
                asset,
            });
    }

    languages_map.into_values().collect()
}

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

async fn download_example(repo_handler: RepoHandler<'_>, asset: Asset) -> Result<()> {
    let out_dir_name = PathBuf::from(asset.name.trim_end_matches(".zip").to_string());
    // This fails if the directory already exists.
    tokio::fs::create_dir(&out_dir_name).await.context(format!(
        "Cannot create the output directory {} for the example",
        out_dir_name.display()
    ))?;

    let mut zip_out_file_path = PathBuf::from(&out_dir_name);
    zip_out_file_path.push("temp.zip");

    // Download zip in the out_dir
    let _ = match download_asset_to_file(&zip_out_file_path, repo_handler, asset).await {
        Ok(f) => f,
        Err(e) => {
            // Try to remove the directory, to avoid leaving a dirty user directory in case of a retry.
            let _ = tokio::fs::remove_dir_all(&out_dir_name).await;
            bail!(
                "Error when downloading the zip {} for the example: {:#?}",
                zip_out_file_path.display(),
                e
            );
        }
    };

    // Unzip it
    if let Err(e) = unzip(&zip_out_file_path, &out_dir_name).await {
        // Try to remove the directory, to avoid leaving a dirty user directory in case of a retry.
        println!("{:#?}", e);
        let _ = tokio::fs::remove_dir_all(&out_dir_name).await;
        return Err(e);
    }

    // Ready to rock!
    c_println!(
        "The example is ready in the directory {}",
        out_dir_name.display()
    );
    c_println!("Look at the example README to get started!");

    Ok(())
}

async fn download_asset_to_file(
    zip_out_file_path: impl AsRef<Path>,
    repo_handler: RepoHandler<'_>,
    asset: Asset,
) -> Result<()> {
    let mut out_file = File::create(zip_out_file_path).await?;
    let mut zip_stream = repo_handler
        .releases()
        .stream_asset(asset.id.clone())
        .await?;
    while let Some(res) = zip_stream.next().await {
        let mut buf = res?;
        out_file.write_buf(&mut buf).await?;
    }
    out_file.flush().await?;

    Ok(())
}

async fn unzip(zip_out_file: &PathBuf, out_name: &PathBuf) -> Result<()> {
    let zip_out_file_copy = zip_out_file.clone();
    let out_dir_copy = out_name.clone();
    tokio::task::spawn_blocking(move || {
        let zip_file = std::fs::File::open(zip_out_file_copy)?;
        let mut archive = zip::ZipArchive::new(zip_file)?;
        archive.extract(out_dir_copy)?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context(format!(
        "Panic when trying to unzip {} in {}",
        zip_out_file.display(),
        out_name.display()
    ))?
    .context(format!(
        "Error when trying to unzip {} in {}",
        zip_out_file.display(),
        out_name.display()
    ))?;

    Ok(())
}
