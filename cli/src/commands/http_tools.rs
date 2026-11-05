// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `restate curl` and `restate http`: run curl / httpie against the ingress of the current
//! profile, with its URL and credential filled in.

use std::ffi::{OsStr, OsString};
use std::process::Command;

use anyhow::Result;
use cling::prelude::*;
use tracing::info;

use crate::cli_env::CliEnv;
use crate::error::{ErrorKind, RestateCliError};

/// Call the ingress with curl, using the current profile's URL and credential
///
/// Runs curl on the ingress of the current profile, with its bearer token (if any) as the
/// Authorization header. Pass the ingress path first: everything after it goes to curl
/// unchanged. Options of `restate`, such as -p/--profile, go before the path.
/// Needs curl on PATH.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_curl")]
#[command(override_usage = "restate curl <PATH> [CURL_ARGS]...")]
#[command(after_help = after_help!(
    examples: [
        "restate curl /restate/call/Greeter/greet --json '\"Alice\"'",
        "restate curl /restate/send/Greeter/greet --json '\"Alice\"' -i",
        "restate -p prod curl /restate/call/Counter/my-key/get",
    ],
    learn_more: "https://docs.restate.dev/services/invocation/http",
))]
pub struct Curl {
    /// The ingress path, e.g. /restate/call/Greeter/greet, followed by curl's arguments
    #[arg(
        required = true,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "ARGS"
    )]
    args: Vec<OsString>,
}

/// Call the ingress with httpie, using the current profile's URL and credential
///
/// Runs httpie's `http` on the ingress of the current profile, with its bearer token (if any)
/// as the Authorization header. Pass the ingress path first, optionally after the method:
/// everything after it goes to httpie unchanged. Options of `restate`, such as -p/--profile, go
/// before the path. Needs httpie on PATH.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_http")]
#[command(override_usage = "restate http [METHOD] <PATH> [HTTPIE_ARGS]...")]
#[command(after_help = after_help!(
    examples: [
        "restate http POST /restate/call/Greeter/greet name=Alice",
        "restate -p prod http /restate/call/Counter/my-key/get",
    ],
    learn_more: "https://docs.restate.dev/services/invocation/http",
))]
pub struct Http {
    /// The method (optional), the ingress path, e.g. /restate/call/Greeter/greet, followed by
    /// httpie's arguments
    #[arg(
        required = true,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "ARGS"
    )]
    args: Vec<OsString>,
}

#[derive(Clone, Copy)]
enum Tool {
    Curl,
    Httpie,
}

impl Tool {
    /// The executable.
    fn program(self) -> &'static str {
        match self {
            Tool::Curl => "curl",
            Tool::Httpie => "http",
        }
    }

    /// The `restate` subcommand running it.
    fn subcommand(self) -> &'static str {
        match self {
            Tool::Curl => "curl",
            Tool::Httpie => "http",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Tool::Curl => "curl",
            Tool::Httpie => "httpie",
        }
    }

    fn install_url(self) -> &'static str {
        match self {
            Tool::Curl => "https://curl.se/download.html",
            Tool::Httpie => "https://httpie.io/cli",
        }
    }
}

pub async fn run_curl(State(env): State<CliEnv>, opts: &Curl) -> Result<()> {
    let (path, rest) = opts
        .args
        .split_first()
        .expect("clap requires at least one argument");
    let url = ingress_url(&env, path, Tool::Curl)?;
    let argv = curl_argv(&url, env.bearer_token()?, rest);
    exec(Tool::Curl, &url, argv)
}

pub async fn run_http(State(env): State<CliEnv>, opts: &Http) -> Result<()> {
    let (method, args) = match opts.args.split_first() {
        Some((first, rest)) if is_method(first) => (Some(first.as_os_str()), rest),
        _ => (None, opts.args.as_slice()),
    };
    let Some((path, rest)) = args.split_first() else {
        return Err(RestateCliError::bad_input(
            "Pass the ingress path after the method, e.g. `restate http POST /restate/call/Greeter/greet`",
        )
        .into());
    };
    let url = ingress_url(&env, path, Tool::Httpie)?;
    let argv = httpie_argv(&url, env.bearer_token()?, method, rest);
    exec(Tool::Httpie, &url, argv)
}

/// The URL of `path` on the ingress of the current profile.
fn ingress_url(env: &CliEnv, path: &OsStr, tool: Tool) -> Result<String> {
    let path = parse_path(path, tool)?;
    let Some(base) = env.ingress_base_url()?.uri() else {
        return Err(RestateCliError::bad_input(format!(
            "The ingress of profile '{}' is a unix socket; `restate {}` needs an HTTP URL",
            env.profile.as_str(),
            tool.subcommand()
        ))
        .into());
    };
    Ok(join_url(&base.to_string(), path))
}

/// The ingress path argument: a path, not a URL (the token must not go to another host), nor an
/// option of the tool (those go after the path).
fn parse_path(path: &OsStr, tool: Tool) -> Result<&str, RestateCliError> {
    let path = path
        .to_str()
        .ok_or_else(|| RestateCliError::bad_input("The ingress path isn't valid UTF-8"))?;
    if path.starts_with('-') {
        return Err(RestateCliError::bad_input(format!(
            "Pass the ingress path first, then {}'s options: `restate {} /restate/call/Greeter/greet {path} ...`",
            tool.name(),
            tool.subcommand()
        )));
    }
    if path.contains("://") {
        return Err(RestateCliError::bad_input(format!(
            "Pass only the path, e.g. /restate/call/Greeter/greet, not the URL {path}: the ingress URL comes from the profile"
        )));
    }
    Ok(path)
}

/// `path` appended to `base`, keeping a path prefix of `base` (unlike `Url::join`).
fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

/// An httpie method (`GET`, `post`, ...): letters only, where a path always has a `/`.
fn is_method(arg: &OsStr) -> bool {
    arg.to_str()
        .is_some_and(|arg| !arg.is_empty() && arg.bytes().all(|b| b.is_ascii_alphabetic()))
}

fn curl_argv(url: &str, token: Option<&str>, rest: &[OsString]) -> Vec<OsString> {
    let mut argv = vec![OsString::from(url)];
    if let Some(token) = token {
        argv.push("-H".into());
        argv.push(format!("Authorization: Bearer {token}").into());
    }
    argv.extend_from_slice(rest);
    argv
}

fn httpie_argv(
    url: &str,
    token: Option<&str>,
    method: Option<&OsStr>,
    rest: &[OsString],
) -> Vec<OsString> {
    let mut argv = Vec::with_capacity(rest.len() + 6);
    if let Some(token) = token {
        argv.extend(["-A", "bearer", "-a", token].map(OsString::from));
    }
    argv.extend(method.map(OsStr::to_owned));
    argv.push(url.into());
    argv.extend_from_slice(rest);
    argv
}

/// Runs the tool in place of this process (on Unix), so its output, exit code and Ctrl-C
/// handling are the tool's own. Only returns on failure to start it.
fn exec(tool: Tool, url: &str, argv: Vec<OsString>) -> Result<()> {
    info!("Running {} against {url}", tool.program());
    let mut command = Command::new(tool.program());
    command.args(argv);

    #[cfg(unix)]
    let err = std::os::unix::process::CommandExt::exec(&mut command);
    #[cfg(not(unix))]
    let err = match command.status() {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(err) => err,
    };

    if err.kind() == std::io::ErrorKind::NotFound {
        return Err(RestateCliError::new(
            ErrorKind::Generic,
            format!(
                "{} isn't installed (`{}` isn't on PATH); install it from {}",
                tool.name(),
                tool.program(),
                tool.install_url()
            ),
        )
        .into());
    }
    Err(anyhow::Error::new(err).context(format!("Failed to run {}", tool.program())))
}

#[cfg(test)]
mod tests {
    use clap::FromArgMatches;

    use super::*;
    use crate::app::{CliApp, Command as AppCommand};

    fn parse(args: &[&str]) -> CliApp {
        let matches = crate::command().try_get_matches_from(args).unwrap();
        CliApp::from_arg_matches(&matches).unwrap()
    }

    fn os(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    #[test]
    fn arguments_after_the_path_go_to_the_tool() {
        let app = parse(&["restate", "curl", "/x", "-p", "foo", "-v"]);
        let AppCommand::Curl(curl) = app.cmd else {
            panic!("not curl");
        };
        assert_eq!(curl.args, os(&["/x", "-p", "foo", "-v"]));
    }

    #[test]
    fn profile_before_the_subcommand_selects_the_profile() {
        let app = parse(&["restate", "-p", "foo", "curl", "/x", "-p", "bar"]);
        assert_eq!(app.global_opts.profile, Some(figment::Profile::new("foo")));
    }

    #[test]
    fn curl_gets_the_url_then_the_token_then_the_user_arguments() {
        let argv = curl_argv("http://h/x", Some("t"), &os(&["--json", "1"]));
        assert_eq!(
            argv,
            os(&["http://h/x", "-H", "Authorization: Bearer t", "--json", "1"])
        );
    }

    #[test]
    fn curl_without_a_token_gets_no_header() {
        let argv = curl_argv("http://h/x", None, &os(&["-i"]));
        assert_eq!(argv, os(&["http://h/x", "-i"]));
    }

    #[test]
    fn httpie_gets_the_token_then_the_method_then_the_url() {
        let argv = httpie_argv(
            "http://h/x",
            Some("t"),
            Some(OsStr::new("POST")),
            &os(&["name=Alice"]),
        );
        assert_eq!(
            argv,
            os(&[
                "-A",
                "bearer",
                "-a",
                "t",
                "POST",
                "http://h/x",
                "name=Alice"
            ])
        );
    }

    #[test]
    fn join_url_keeps_the_base_path() {
        assert_eq!(
            join_url("https://h/prefix/", "restate/call/A/b?x=1"),
            "https://h/prefix/restate/call/A/b?x=1"
        );
    }

    #[test]
    fn a_full_url_is_rejected() {
        assert!(parse_path(OsStr::new("https://evil/x"), Tool::Curl).is_err());
    }
}
