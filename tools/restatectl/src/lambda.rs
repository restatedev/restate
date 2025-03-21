use std::collections::HashMap;

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

/// Turn this binary into a valid lambda bootstrap binary - if lambda environment variables are present,
/// and the executable name is 'bootstrap' (required by lambda anyway), then instead of executing the CLI
/// as normal, we will start a lambda handler which can then exec itself without lambda environment variables
/// to trigger normal restatectl operations.
///
/// To use, simply put a restatectl binary named 'bootstrap' into a zip and upload it as a lambda.
pub async fn handle_lambda_if_needed() {
    if std::env::var("AWS_LAMBDA_FUNCTION_NAME").is_err() {
        return;
    }

    let Ok(exe) = std::env::current_exe() else {
        return;
    };

    let Some("bootstrap") = exe.file_name().and_then(std::ffi::OsStr::to_str) else {
        return;
    };

    let func = lambda_runtime::service_fn(|event: LambdaEvent<Request>| async {
        eprintln!("Executing restatectl {}", event.payload.args.join(" "));

        let mut cmd = tokio::process::Command::new(exe.as_os_str());

        cmd.args(event.payload.args);

        for (key, value) in event.payload.env.unwrap_or_default() {
            cmd.env(key, value);
        }

        cmd.env_remove("AWS_LAMBDA_FUNCTION_NAME"); // so that we don't just hit this code path again

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
    });

    match lambda_runtime::run(func).await {
        Ok(()) => std::process::exit(0),
        Err(err) => {
            eprintln!("Failed to run lambda handler: {err}");
            std::process::exit(1)
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),
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
