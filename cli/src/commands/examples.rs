// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use cling::prelude::*;
use convert_case::{Case, Casing};
use futures::StreamExt;
use octocrab::models::repos::Asset;
use octocrab::repos::RepoHandler;
use restate_cli_util::ui::console::input;
use restate_cli_util::ui::stylesheet::Style;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::console::{Styled, c_println, choose};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_examples")]
pub struct Examples {
    /// Output directory.
    #[arg(long, alias = "out")]
    output_directory: Option<PathBuf>,

    /// Example name.
    ///
    /// If omitted, an interactive prompt will ask you which example to download.
    name: Option<String>,
}

pub async fn run_examples(example_opts: &Examples) -> Result<()> {
    let octocrab = octocrab::instance();
    let examples_repo = octocrab.repos("restatedev", "examples");
    let ai_examples_repo = octocrab.repos("restatedev", "ai-examples");

    // Fetch latest releases from both repos in parallel
    let examples_releases = examples_repo.releases();
    let ai_examples_releases = ai_examples_repo.releases();
    let (examples_release, ai_examples_release) = tokio::join!(
        examples_releases.get_latest(),
        ai_examples_releases.get_latest()
    );

    let examples_assets = examples_release
        .context("Can't access the examples releases. Check if your machine can access the Github repository https://github.com/restatedev/examples")?
        .assets;

    // ai-examples repo might not have releases yet, treat as empty
    let ai_examples_assets = ai_examples_release
        .map(|r| r.assets)
        .unwrap_or_default();

    let (selected_example, selected_repo) = if let Some(example) = &example_opts.name {
        // Check if the example exists, prefer examples repo if found in both
        let example_lowercase = example.to_lowercase();

        let from_examples = examples_assets
            .iter()
            .find(|a| a.name.trim_end_matches(".zip") == example_lowercase);
        let from_ai_examples = ai_examples_assets
            .iter()
            .find(|a| a.name.trim_end_matches(".zip") == example_lowercase);

        match (from_examples, from_ai_examples) {
            (Some(asset), _) => (asset.clone(), ExampleRepo::Examples),
            (None, Some(asset)) => (asset.clone(), ExampleRepo::AiExamples),
            (None, None) => {
                bail!(
                    "Unknown example {}. Use `restate example` to navigate the list of examples.",
                    example_lowercase
                );
            }
        }
    } else {
        // Ask the example to download among the available examples
        let mut languages = parse_available_examples(examples_assets, ai_examples_assets);
        let language_selection = choose("Choose the programming language", &languages)?;
        let example_selection = choose(
            "Choose the example",
            &languages[language_selection].examples,
        )?;

        let example = languages
            .remove(language_selection)
            .examples
            .remove(example_selection);
        (example.asset, example.repo)
    };

    let output_dir = if let Some(out_dir) = &example_opts.output_directory {
        out_dir.clone()
    } else {
        input(
            "Output directory",
            selected_example.name.trim_end_matches(".zip").to_owned(),
        )?
        .into()
    };

    let repo_handler = match selected_repo {
        ExampleRepo::Examples => examples_repo,
        ExampleRepo::AiExamples => ai_examples_repo,
    };

    download_example(output_dir, repo_handler, selected_example).await
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

impl Language {
    fn reorder_examples(&mut self) {
        // Sorts by putting examples starting with Hello World at the beginning, and preserves the order of the other examples
        self.examples.sort_by(|a, b| {
            match (
                a.display_name.starts_with("Hello World"),
                b.display_name.starts_with("Hello World"),
            ) {
                (true, true) => a.display_name.cmp(&b.display_name),
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => Ordering::Equal,
            }
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExampleRepo {
    Examples,
    AiExamples,
}

struct Example {
    display_name: String,
    asset: Asset,
    repo: ExampleRepo,
}

impl fmt::Display for Example {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

fn parse_available_examples(
    examples_assets: Vec<Asset>,
    ai_examples_assets: Vec<Asset>,
) -> Vec<Language> {
    let mut languages_map = HashMap::new();

    let tagged_assets = examples_assets
        .into_iter()
        .map(|a| (a, ExampleRepo::Examples))
        .chain(
            ai_examples_assets
                .into_iter()
                .map(|a| (a, ExampleRepo::AiExamples)),
        );

    for (asset, repo) in tagged_assets {
        // Zip names have the format [language]-[example_name].zip

        let asset_name = asset.name.clone();
        let mut split_asset_name = asset_name.splitn(2, '-');

        // First part is language
        let Some(language) = split_asset_name.next() else {
            // Bad name, just ignore it
            continue;
        };
        let language_display_name = capitalize(language);

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
        let example_display_name = example_display_name.trim_end_matches(".zip").to_owned();

        languages_map
            .entry(language.to_owned())
            .or_insert_with(|| Language {
                display_name: language_display_name,
                examples: vec![],
            })
            .examples
            .push(Example {
                display_name: example_display_name,
                asset,
                repo,
            });
    }

    for language in languages_map.values_mut() {
        language.reorder_examples();
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

async fn download_example(
    out_dir_name: PathBuf,
    repo_handler: RepoHandler<'_>,
    asset: Asset,
) -> Result<()> {
    // This fails if the directory already exists.
    tokio::fs::create_dir(&out_dir_name)
        .await
        .with_context(|| {
            format!(
                "Cannot create the output directory {} for the example",
                out_dir_name.display()
            )
        })?;
    c_println!("Created directory {}", out_dir_name.display());

    let mut zip_out_file_path = PathBuf::from(&out_dir_name);
    zip_out_file_path.push("temp.zip");

    // Download zip in the out_dir
    match download_asset_to_file(&zip_out_file_path, repo_handler, asset).await {
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
    c_println!("Downloaded example zip in {}", zip_out_file_path.display());

    // Unzip it
    if let Err(e) = unzip(&zip_out_file_path, &out_dir_name).await {
        // Try to remove the directory, to avoid leaving a dirty user directory in case of a retry.
        let _ = tokio::fs::remove_dir_all(&out_dir_name).await;
        return Err(e);
    }

    // Remove the zip file
    if (tokio::fs::remove_file(&zip_out_file_path).await).is_err() {
        c_println!(
            "{} Couldn't cleanup the zip file {}",
            Styled(Style::Warn, "Warning:"),
            zip_out_file_path.display()
        )
    }

    // Ready to rock!
    c_println!(
        "The example is ready in the directory {}",
        Styled(Style::Success, out_dir_name.display())
    );
    c_println!(
        "Look at the {}/README.md to get started!",
        out_dir_name.display()
    );

    Ok(())
}

async fn download_asset_to_file(
    zip_out_file_path: impl AsRef<Path>,
    repo_handler: RepoHandler<'_>,
    asset: Asset,
) -> Result<()> {
    let mut out_file = File::create(zip_out_file_path).await?;
    let mut zip_stream = repo_handler.release_assets().stream(*asset.id).await?;
    while let Some(res) = zip_stream.next().await {
        let mut buf = res?;
        out_file.write_buf(&mut buf).await?;
    }
    out_file.flush().await?;

    Ok(())
}

async fn unzip(zip_out_file: &Path, out_name: &Path) -> Result<()> {
    let zip_out_file_copy = zip_out_file.to_owned();
    let out_dir_copy = out_name.to_owned();
    tokio::task::spawn_blocking(move || {
        let zip_file = std::fs::File::open(zip_out_file_copy)?;
        let mut archive = zip::ZipArchive::new(zip_file)?;
        archive.extract(out_dir_copy)?;
        Ok::<_, anyhow::Error>(())
    })
    .await
    .with_context(|| {
        format!(
            "Panic when trying to unzip {} in {}",
            zip_out_file.display(),
            out_name.display()
        )
    })?
    .with_context(|| {
        format!(
            "Error when trying to unzip {} in {}",
            zip_out_file.display(),
            out_name.display()
        )
    })?;

    Ok(())
}
