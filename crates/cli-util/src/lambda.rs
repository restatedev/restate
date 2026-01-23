// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use cling::{Collect, Run, prelude::Parser};
use lambda_runtime::LambdaEvent;

#[derive(serde::Deserialize)]
struct Request {
    args: Vec<String>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
}

#[derive(serde::Serialize)]
struct Response {
    status: i32,
    stdout: String,
    stderr: String,
}

#[derive(Run, Parser, Collect, Clone, Debug)]
#[command(hide = true)]
#[cling(run = "lambda")]
pub struct LambdaOpts;

struct LambdaState {
    exe: std::path::PathBuf,
    filename: String,
}

/// Start a Lambda runtime handling incoming requests by executing them as sub-commands of this binary
async fn lambda() -> anyhow::Result<()> {
    let exe = std::env::current_exe()?;
    let filename = exe
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("unknown")
        .to_owned();

    let state = std::sync::Arc::new(LambdaState { exe, filename });

    let func = lambda_runtime::service_fn({
        |event: LambdaEvent<Request>| {
            let state = state.clone();
            async move {
                eprintln!(
                    "Executing {} {}",
                    state.filename,
                    event.payload.args.join(" ")
                );

                let mut cmd = tokio::process::Command::new(state.exe.as_os_str());

                cmd.args(event.payload.args);

                for (key, value) in event.payload.env.unwrap_or_default() {
                    cmd.env(key, value);
                }

                cmd.stdout(std::process::Stdio::piped());
                cmd.stderr(std::process::Stdio::piped());

                let child = cmd.spawn()?;
                let output = child.wait_with_output().await?;

                let status = output.status.code().unwrap_or(-1);
                let stdout = String::from_utf8_lossy(&output.stdout).into();
                let stderr = String::from_utf8_lossy(&output.stderr).into();

                eprintln!("Exit status {status}");
                eprintln!("STDERR: {stderr}");
                eprintln!("STDOUT: {stdout}");

                Result::<Response, Error>::Ok(Response {
                    status,
                    stdout,
                    stderr,
                })
            }
        }
    });

    match lambda_runtime::run(func).await {
        Ok(()) => Ok(()),
        Err(err) => Err(anyhow::anyhow!(err)),
    }
}

#[derive(Debug)]
enum Error {
    IO(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<Error> for lambda_runtime::Diagnostic {
    fn from(value: Error) -> Self {
        match value {
            Error::IO(err) => Self {
                error_type: "IO".into(),
                error_message: err.to_string(),
            },
        }
    }
}
