// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Result;
use clap::CommandFactory;
use cling::prelude::*;
use figment::Profile;
use tracing::info;

use restate_cli_util::{CliContext, CommonOpts};

use crate::cli_env::{CliEnv, EnvironmentSource};
use crate::commands::completions::Completions;
use crate::commands::*;

/// Restate command-line interface
///
/// Manage and inspect a Restate server: register service deployments, look into and act on
/// invocations, read and edit state, and query the server's internals with SQL.
///
/// Docs: https://docs.restate.dev
#[derive(Run, Parser, Clone)]
#[command(author, version = crate::build_info::version(), infer_subcommands = true)]
#[command(after_help = ROOT_HELP, after_long_help = ROOT_LONG_HELP)]
#[cling(run = "init")]
pub struct CliApp {
    #[clap(flatten)]
    pub common_opts: CommonOpts,
    #[clap(flatten)]
    pub global_opts: GlobalOpts,
    #[clap(subcommand)]
    pub cmd: Command,
}

/// `restate --help` and `restate -h`, with the notes for agents and scripts before the commands.
const ROOT_HELP_TEMPLATE: &str = concat!(
    "{before-help}{about-with-newline}\n{usage-heading} {usage}\n\n",
    heading!("For AI agents and scripts:"),
    "
  - Add --json to any command for a single JSON document on stdout, errors included.
    Logs and diagnostics go to stderr.
  - Not sure which command to use? Run `restate search <what you want to do>`.
    `restate sql --help` lists the SQL tables, `restate openapi` prints the admin API spec.
  - Commands that change something show the planned changes and ask for confirmation. They
    accept --dry-run to only preview the changes, and --yes to apply them without asking.
  - On failure the exit code is non-zero, and with --json the error document has a `kind`.
  - Commands never prompt with --json, with --non-interactive, when stdin is not a terminal,
    or when $CI is set (to anything but `false` or `0`). $CI also implies --yes.

{all-args}{after-help}",
);

const ROOT_HELP: &str = after_help!(learn_more: "https://docs.restate.dev/");

const ROOT_LONG_HELP: &str = concat!(
    heading!("Connecting to a server:"),
    "
  By default the CLI talks to a server on this machine (admin API at http://localhost:9070).
  To target another one, set $RESTATE_ADMIN_URL, plus $RESTATE_AUTH_TOKEN for a bearer token:
    RESTATE_ADMIN_URL=https://restate.example.com:9070 RESTATE_AUTH_TOKEN=... restate whoami
  or add an environment to the CLI config file (see `restate config --help`) and select it
  with -e/--environment or `restate config use-environment`. The variables take precedence
  over the config file.

",
    after_help!(learn_more: "https://docs.restate.dev/"),
);

/// Global options also listed in subcommand help, the ones agents and scripts need most.
/// `restate --help` lists all of them.
const SUBCOMMAND_HELP_GLOBALS: [&str; 3] = ["json", "yes", "environment"];

const SUBCOMMAND_HELP_TEMPLATE: &str = "\
{before-help}{about-with-newline}
{usage-heading} {usage}

{all-args}

More global options (verbosity, colors, timeouts, ...): `restate --help`.{after-help}";

/// The clap command of [`CliApp`], with subcommand help trimmed to the most used global
/// options (see [`SUBCOMMAND_HELP_GLOBALS`]). Use it instead of `CliApp::command()`.
pub fn command() -> clap::Command {
    let cmd = CliApp::command();
    // Subcommands get the other global options as copies hidden from help: clap then
    // propagates these instead of the visible ones, and parsing is unchanged.
    let hidden: Vec<clap::Arg> = cmd
        .get_arguments()
        .filter(|arg| {
            arg.is_global_set() && !SUBCOMMAND_HELP_GLOBALS.contains(&arg.get_id().as_str())
        })
        .map(|arg| arg.clone().hide_short_help(true).hide_long_help(true))
        .collect();
    cmd.help_template(ROOT_HELP_TEMPLATE)
        .mut_subcommands(|sub| trim_global_help(sub, &hidden))
}

fn trim_global_help(cmd: clap::Command, hidden: &[clap::Arg]) -> clap::Command {
    hidden
        .iter()
        .fold(cmd, |cmd, arg| cmd.arg(arg.clone()))
        .help_template(SUBCOMMAND_HELP_TEMPLATE)
        .mut_subcommands(|sub| trim_global_help(sub, hidden))
}

#[derive(Args, Collect, Clone, Default)]
#[command(next_help_heading = "Global options")]
pub struct GlobalOpts {
    /// Environment (a section of the CLI config file) to use. When omitted: $RESTATE_ENVIRONMENT,
    /// else the one selected with `restate config use-environment`, else `local` (a server on
    /// this machine). List them with `restate config list-environments`.
    #[arg(long, short, global = true, display_order = 0)]
    pub environment: Option<Profile>,
}

#[derive(Run, Subcommand, Clone)]
pub enum Command {
    #[cfg(feature = "dev-cmd")]
    #[clap(name = "dev", visible_alias = "up")]
    Dev(dev::Dev),

    #[clap(name = "whoami")]
    WhoAmI(whoami::WhoAmI),
    /// Inspect registered services and change their configuration
    ///
    /// Your business logic lives in services: regular applications that embed the Restate SDK.
    /// Services contain handlers (durable functions) that process requests and execute business logic.
    #[clap(subcommand)]
    #[command(after_help = after_help!(
        learn_more: "https://docs.restate.dev/foundations/services",
    ))]
    Services(services::Services),
    /// Register, inspect and remove service deployments
    ///
    /// A deployment is a version of your service(s) code that Restate calls.
    /// Registering it makes Restate discover its services and route new invocations to them,
    /// while invocations keep running on the deployment they started on.
    #[clap(subcommand)]
    #[command(after_help = after_help!(
        learn_more: "https://docs.restate.dev/services/versioning",
    ))]
    Deployments(deployments::Deployments),
    /// Manage Kafka clusters
    #[clap(subcommand)]
    KafkaClusters(kafkaclusters::KafkaClusters),
    /// Manage Kafka subscriptions
    #[clap(subcommand)]
    Subscriptions(subscriptions::Subscriptions),
    /// Inspect and manage invocations: list, describe, cancel, kill, pause, resume, ...
    ///
    /// An invocation is one request to a handler, with an `inv_...` id.
    #[clap(subcommand)]
    #[command(after_help = after_help!(
        learn_more: "https://docs.restate.dev/services/invocation/managing-invocations#lifecycle",
    ))]
    Invocations(invocations::Invocations),
    /// Inspect virtual queues, where invocations wait for their turn to run
    ///
    /// Each virtual queue holds the invocations of one service that share the same scope, limit
    /// key and, for virtual objects, object key. Useful to see what's waiting on a concurrency
    /// limit (see `restate rules`) or on a busy virtual object key.
    #[clap(name = "vqueues", subcommand)]
    #[command(after_help = after_help!(
        learn_more: "https://docs.restate.dev/services/flow-control",
    ))]
    VQueues(vqueues::VQueues),
    /// Manage concurrency-limit rules (flow control)
    ///
    /// A rule caps how many invocations can run at the same time in a scope. Invocations get a
    /// scope, and optionally a limit key, when sent through a scoped ingress endpoint
    /// (`/restate/scope/<scope>/...`); rules don't match service names. A pattern is `scope`,
    /// `scope/l1` or `scope/l1/l2`, where each part is an exact value or `*`, and the most
    /// specific matching rule applies. The limit applies to each matching scope separately:
    /// `*` gives every scope its own budget, it's not a global limit.
    #[clap(subcommand)]
    #[command(after_help = after_help!(
        learn_more: "https://docs.restate.dev/services/flow-control",
    ))]
    Rules(rules::Rules),
    Sql(sql::Sql),
    Search(search::Search),
    #[clap(name = "openapi")]
    OpenApi(openapi::OpenApi),
    #[clap(name = "example", visible_aliases = ["examples", "template", "templates"])]
    Examples(examples::Examples),

    /// Read and change the K/V state of virtual objects and workflows
    ///
    /// Each virtual object or workflow instance has its own state: a set of key/value pairs, where
    /// values are usually JSON.
    #[clap(name = "state", alias = "kv")]
    #[clap(subcommand)]
    #[command(after_help = after_help!(
        learn_more: "https://docs.restate.dev/foundations/key-concepts#consistent-state",
    ))]
    State(state::ServiceState),

    #[clap(subcommand)]
    Completions(Completions),

    /// Manage the CLI config file and its environments (servers to talk to)
    #[clap(subcommand, alias = "conf")]
    #[command(verbatim_doc_comment)]
    #[command(after_help = after_help!(
        learn_more: "https://docs.restate.dev/references/cli-config",
    ))]
    Config(config::Config),

    #[cfg(feature = "cloud")]
    #[clap(subcommand)]
    /// Manage Restate Cloud
    Cloud(cloud::Cloud),

    /// Run as an AWS Lambda server
    Lambda(restate_cli_util::lambda::LambdaOpts),
}

fn init(common_opts: &CommonOpts, global_opts: &GlobalOpts) -> Result<State<CliEnv>> {
    CliContext::new(common_opts.clone()).set_as_global();
    let env = CliEnv::load(global_opts)?;

    match &env.environment_source {
        EnvironmentSource::Argument => {
            info!("Using environment from --environment")
        }
        EnvironmentSource::Environment => {
            info!("Using environment from $RESTATE_ENVIRONMENT")
        }
        EnvironmentSource::File => {
            info!("Using environment from {}", env.environment_file.display())
        }
        EnvironmentSource::None => {
            info!("Didn't load an environment")
        }
    }

    Ok(State(env))
}
