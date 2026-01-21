// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains a collection of utils to interact with the filesystem.

use std::env::temp_dir;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use tokio::io;
use tracing::{info, trace};

/// Generate a temp dir name but doesn't create it!
pub fn generate_temp_dir_name(prefix: &'static str) -> PathBuf {
    temp_dir().join(format!("{}-{}", prefix, uuid::Uuid::now_v7().simple()))
}

pub async fn initialize_temp_dir(prefix: &'static str) -> io::Result<PathBuf> {
    let new_tmp_dir = generate_temp_dir_name(prefix);
    tokio::fs::create_dir_all(&new_tmp_dir).await?;
    Ok(new_tmp_dir)
}

/// Variant of [`tokio::fs::create_dir_all`] that won't fail if the directory already exists.
pub async fn create_dir_all_if_doesnt_exists(p: impl AsRef<Path>) -> io::Result<()> {
    match tokio::fs::create_dir_all(&p).await {
        Ok(_) => {
            trace!("Created directory {}", p.as_ref().display());
            Ok(())
        }
        Err(io_err) => match io_err.kind() {
            ErrorKind::AlreadyExists => {
                // Can ignore
                Ok(())
            }
            _ => Err(io_err),
        },
    }
}

/// Variant of [`tokio::fs::remove_file`] that won't fail if the file doesn't exists.
pub async fn remove_file_if_exists(p: impl AsRef<Path>) -> io::Result<()> {
    match tokio::fs::remove_file(&p).await {
        Ok(_) => {
            info!("Removed file {}", p.as_ref().display());
            Ok(())
        }
        Err(io_err) => match io_err.kind() {
            ErrorKind::NotFound => {
                // Can ignore
                Ok(())
            }
            _ => Err(io_err),
        },
    }
}

/// Variant of [`tokio::fs::remove_dir_all`] that won't fail if the directory doesn't exists.
pub async fn remove_dir_all_if_exists(p: impl AsRef<Path>) -> io::Result<()> {
    match tokio::fs::remove_dir_all(&p).await {
        Ok(_) => {
            info!("Removed directory {}", p.as_ref().display());
            Ok(())
        }
        Err(io_err) => match io_err.kind() {
            ErrorKind::NotFound => {
                // Can ignore
                Ok(())
            }
            _ => Err(io_err),
        },
    }
}
