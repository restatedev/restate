// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    ffi::OsString,
    fmt::Display,
    future::Future,
    io::{self, ErrorKind},
    net::SocketAddr,
    ops::{Deref, DerefMut},
    path::PathBuf,
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use arc_swap::ArcSwapOption;
use enumset::{enum_set, EnumSet};
use futures::{stream, FutureExt, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use regex::{Regex, RegexSet};
use rev_lines::RevLines;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::mpsc,
    task::JoinHandle,
};
use tokio::{process::Command, sync::mpsc::Sender};
use tracing::{error, info, warn};
use typed_builder::TypedBuilder;

use restate_types::{
    config::{Configuration, MetadataStoreClient},
    errors::GenericError,
    metadata_store::keys::NODES_CONFIG_KEY,
    net::{AdvertisedAddress, BindAddress},
    nodes_config::{NodesConfiguration, Role},
    PlainNodeId,
};

use crate::random_socket_address;

#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
pub struct Node {
    #[builder(mutators(
            #[mutator(requires = [base_dir])]
            pub fn with_node_socket(self) {
                let node_socket: PathBuf = PathBuf::from(self.base_config.node_name()).join("node.sock");
                let tokio_console_socket: PathBuf = PathBuf::from(self.base_config.node_name()).join("tokio_console.sock");
                self.base_config.common.bind_address = Some(BindAddress::Uds(node_socket.clone()));
                self.base_config.common.tokio_console_bind_address = Some(BindAddress::Uds(tokio_console_socket));
                self.base_config.common.advertised_address = AdvertisedAddress::Uds(node_socket);
            }

            #[mutator(requires = [base_dir])]
            pub fn with_socket_metadata(self) {
                let metadata_socket: PathBuf = "metadata.sock".into();
                self.base_config.metadata_store.bind_address = BindAddress::Uds(metadata_socket.clone());
                self.base_config.common.metadata_store_client.metadata_store_client = MetadataStoreClient::Embedded { address: AdvertisedAddress::Uds(metadata_socket) }
            }

            pub fn with_random_ports(self) {
                self.base_config.admin.bind_address =
                    random_socket_address().expect("to find a random port for the admin server");
                self.base_config.admin.query_engine.pgsql_bind_address =
                    random_socket_address().expect("to find a random port for the pgsql server");
                self.base_config.ingress.bind_address =
                    random_socket_address().expect("to find a random port for the ingress server");
            }

            pub fn with_node_name(self, node_name: impl Into<String>) {
                self.base_config.common.set_node_name(node_name.into());
            }

            pub fn with_node_id(self, node_id: PlainNodeId) {
                self.base_config.common.force_node_id = Some(node_id)
            }

            pub fn with_roles(self, roles: EnumSet<Role>) {
                self.base_config.common.roles = roles
            }
    ))]
    base_config: Configuration,
    binary_source: BinarySource,
    #[builder(default)]
    args: Vec<String>,
    #[builder(default = true)]
    inherit_env: bool,
    #[builder(default)]
    env: Vec<(String, String)>,
    #[builder(default)]
    #[serde(skip)]
    searcher: Searcher,
}

#[derive(Debug, thiserror::Error)]
pub enum NodeStartError {
    #[error("Failed to absolutize node base path: {0}")]
    Absolute(io::Error),
    #[error(transparent)]
    BinarySourceError(#[from] BinarySourceError),
    #[error("Failed to create node base directory: {0}")]
    CreateDirectory(io::Error),
    #[error("Failed to create or truncate node config file: {0}")]
    CreateConfig(io::Error),
    #[error("Failed to create or append to node log file: {0}")]
    CreateLog(io::Error),
    #[error("Failed to dump config to bytes: {0}")]
    DumpConfig(GenericError),
    #[error("Failed to spawn restate-server: {0}")]
    SpawnError(io::Error),
}

impl Node {
    pub fn node_name(&self) -> &str {
        self.base_config.node_name()
    }

    pub fn config(&self) -> &Configuration {
        &self.base_config
    }

    pub fn config_mut(&mut self) -> &mut Configuration {
        &mut self.base_config
    }

    // Creates a new Node that uses a node socket, a metadata socket, and random ports,
    // suitable for running in a cluster. Node name, roles, bind/advertise addresses,
    // and the metadata address from the base_config will all be overwritten.
    pub fn new_test_node(
        node_name: impl Into<String>,
        base_config: Configuration,
        binary_source: BinarySource,
        roles: EnumSet<Role>,
    ) -> Self {
        Self::builder()
            .binary_source(binary_source)
            .base_config(base_config)
            .with_node_name(node_name)
            .with_node_socket()
            .with_socket_metadata()
            .with_random_ports()
            .with_roles(roles)
            .build()
    }

    // Creates a group of Nodes with a single metadata node "metadata-node" running the
    // metadata-store and the admin role, and a given number of other nodes ["node-1", ..] each with
    // the provided roles. Node name, roles, bind/advertise addresses, and the metadata address from
    // the base_config will all be overwritten.
    pub fn new_test_nodes_with_metadata(
        base_config: Configuration,
        binary_source: BinarySource,
        roles: EnumSet<Role>,
        size: u32,
    ) -> Vec<Self> {
        let mut nodes = Vec::with_capacity((size + 1) as usize);

        {
            let mut base_config = base_config.clone();
            // let any node write the initial NodesConfiguration
            base_config.common.allow_bootstrap = true;
            base_config.common.force_node_id = Some(PlainNodeId::new(0));
            nodes.push(Self::new_test_node(
                "metadata-node",
                base_config,
                binary_source.clone(),
                enum_set!(Role::Admin | Role::MetadataStore),
            ));
        }

        for node in 1..=size {
            let mut base_config = base_config.clone();
            base_config.common.force_node_id = Some(PlainNodeId::new(node));
            base_config.ingress.bind_address = SocketAddr::new(
                "127.0.0.1".parse().unwrap(),
                8080 - 1 + u16::try_from(node).unwrap(),
            );

            // Create a separate ingress role when running a worker
            let roles = if roles.contains(Role::Worker) {
                base_config
                    .ingress
                    .experimental_feature_enable_separate_ingress_role = true;
                roles | Role::HttpIngress
            } else {
                roles
            };

            nodes.push(Self::new_test_node(
                format!("node-{node}"),
                base_config,
                binary_source.clone(),
                roles,
            ));
        }

        nodes
    }

    /// Start this Node, providing the base_dir and the cluster_name of the cluster its
    /// expected to attach to. All relative file paths addresses specified in the node config
    /// (eg, nodename/node.sock) will be absolutized against the base path, and the base dir
    /// and cluster name present in config will be overwritten.
    pub async fn start_clustered(
        mut self,
        base_dir: impl Into<PathBuf>,
        cluster_name: impl Into<String>,
    ) -> Result<StartedNode, NodeStartError> {
        let base_dir = base_dir.into();

        // ensure file paths are relative to the base dir
        if let MetadataStoreClient::Embedded {
            address: AdvertisedAddress::Uds(file),
        } = &mut self
            .base_config
            .common
            .metadata_store_client
            .metadata_store_client
        {
            *file = base_dir.join(&*file)
        }
        if let BindAddress::Uds(file) = &mut self.base_config.metadata_store.bind_address {
            *file = base_dir.join(&*file)
        }

        if self.base_config.common.bind_address.is_none() {
            // Derive bind_address from advertised_address
            self.base_config.common.bind_address = Some(
                self.base_config
                    .common
                    .advertised_address
                    .derive_bind_address(),
            );
        }

        if let Some(BindAddress::Uds(file)) = &mut self.base_config.common.bind_address {
            *file = base_dir.join(&*file);
        }
        if let AdvertisedAddress::Uds(file) = &mut self.base_config.common.advertised_address {
            *file = base_dir.join(&*file)
        }
        if let Some(BindAddress::Uds(file)) =
            &mut self.base_config.common.tokio_console_bind_address
        {
            *file = base_dir.join(&*file)
        }

        self.base_config.common.set_base_dir(base_dir);
        self.base_config.common.set_cluster_name(cluster_name);

        self.start().await
    }

    /// Start this node with the current config. A subprocess will be created, and a tokio task
    /// spawned to process output logs and watch for exit.
    pub async fn start(self) -> Result<StartedNode, NodeStartError> {
        let Self {
            base_config,
            binary_source,
            args,
            inherit_env,
            env,
            searcher,
        } = self;

        let node_base_dir = std::path::absolute(
            base_config
                .common
                .base_dir()
                .join(base_config.common.node_name()),
        )
        .map_err(NodeStartError::Absolute)?;

        if !node_base_dir.exists() {
            std::fs::create_dir_all(&node_base_dir).map_err(NodeStartError::CreateDirectory)?;
        }
        let node_config_file = node_base_dir.join("config.toml");

        {
            let config_dump = base_config.dump().map_err(NodeStartError::DumpConfig)?;

            let mut config_file = File::create(&node_config_file)
                .await
                .map_err(NodeStartError::CreateConfig)?;

            config_file
                .write_all(config_dump.as_bytes())
                .await
                .map_err(NodeStartError::CreateConfig)?;
        }

        let node_log_filename = node_base_dir.join("restate.log");

        let node_log_file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&node_log_filename)
            .await
            .map_err(NodeStartError::CreateLog)?;

        let binary_path: OsString = binary_source.try_into()?;
        let mut cmd = Command::new(&binary_path);

        if !inherit_env {
            cmd.env_clear()
        } else {
            &mut cmd
        }
        .env("RESTATE_CONFIG", node_config_file)
        .envs(env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0) // avoid terminal control C being propagated
        .args(&args);

        let mut child = cmd.spawn().map_err(NodeStartError::SpawnError)?;
        let pid = child.id().expect("child to have a pid");
        info!(
            "Started node {} in {} (pid {pid})",
            base_config.node_name(),
            node_base_dir.display(),
        );
        let stdout = child.stdout.take().expect("child to have a stdout pipe");
        let stderr = child.stderr.take().expect("child to have a stderr pipe");

        let stdout_reader =
            stream::try_unfold(BufReader::new(stdout).lines(), |mut lines| async move {
                match lines.next_line().await {
                    Ok(Some(line)) => Ok(Some((line, lines))),
                    Ok(None) => Ok(None),
                    Err(err) => Err(err),
                }
            });
        let stderr_reader =
            stream::try_unfold(BufReader::new(stderr).lines(), |mut lines| async move {
                match lines.next_line().await {
                    Ok(Some(line)) => Ok(Some((line, lines))),
                    Ok(None) => Ok(None),
                    Err(err) => Err(err),
                }
            });

        let lines = futures::stream::select(stdout_reader, stderr_reader);

        let node_name = base_config.node_name().to_owned();

        let lines_fut = {
            let searcher = searcher.clone();
            let node_name = node_name.clone();
            let forward_logs = std::env::var("LOCAL_CLUSTER_RUNNER_FORWARD_LOGS")
                .map(|s| s == "true" || s == "1")
                .unwrap_or(false);
            async move {
                let searcher = &searcher;
                let node_name = node_name.as_str();
                let mut node_log_file = lines
                    .try_fold(node_log_file, |mut node_log_file, line| async move {
                        if forward_logs {
                            eprintln!("{node_name}	| {line}")
                        }
                        node_log_file.write_all(line.as_bytes()).await?;
                        node_log_file.write_u8(b'\n').await?;
                        searcher.matches(line.as_str()).await;
                        Ok(node_log_file)
                    })
                    .await?;
                node_log_file.flush().await?;
                searcher.close();
                io::Result::Ok(())
            }
        };

        let child_handle = tokio::spawn(async move {
            let (status, _) = tokio::join!(child.wait(), lines_fut);

            match status {
                Ok(status) => {
                    info!("Node {} exited with {status}", node_name);
                }
                Err(ref err) => {
                    error!(
                        "Node {} exit status could not be determined: {err}",
                        node_name
                    );
                }
            }

            status
        });

        Ok(StartedNode {
            log_file: node_log_filename,
            status: StartedNodeStatus::Running {
                child_handle,
                searcher,
                pid,
            },
            config: base_config,
        })
    }

    /// Obtain a stream of loglines matching this pattern. The stream will end
    /// when the stdout and stderr files on the process close.
    pub fn lines(&self, pattern: Regex) -> impl Stream<Item = String> + 'static {
        self.searcher.search(pattern)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinarySource {
    Path(OsString),
    EnvVar(String),
    /// Suitable when called from a `cargo run` command, except examples.
    /// This will attempt to find a `restate-server` binary in the same directory
    /// as the current binary
    CargoRun,
    /// Suitable when called from a `cargo test` or `cargo run --example` command;
    /// this will attempt to find a `restate-server` binary in the parent directory of
    /// the current binary.
    CargoTest,
}

#[derive(Debug, thiserror::Error)]
pub enum BinarySourceError {
    #[error("The env var {0} is not present, can't use it to find a restate binary")]
    MissingEnvVar(String),
    #[error("Could not find the path to restate-server - this may not be a `cargo run` call")]
    NotACargoRun,
    #[error("Could not find the path to restate-server - this may not be a `cargo test` call")]
    NotACargoTest,
}

impl TryInto<OsString> for BinarySource {
    type Error = BinarySourceError;

    fn try_into(self) -> Result<OsString, Self::Error> {
        match self {
            BinarySource::Path(p) => Ok(p),
            BinarySource::EnvVar(var) => {
                std::env::var_os(&var).ok_or(BinarySourceError::MissingEnvVar(var))
            }
            BinarySource::CargoRun => {
                // Cargo puts the run binary in target/debug
                let test_bin_dir = std::env::current_exe()
                    .ok()
                    .and_then(|d| {
                        d.parent() // debug
                            .map(|d| d.to_owned())
                    })
                    .ok_or(BinarySourceError::NotACargoRun)?;

                let bin = test_bin_dir.join("restate-server");
                if !bin.exists() {
                    return Err(BinarySourceError::NotACargoRun);
                }

                Ok(bin.into())
            }
            BinarySource::CargoTest => {
                // Cargo puts the test binary in target/debug/deps
                let test_bin_dir = std::env::current_exe()
                    .ok()
                    .and_then(|d| {
                        d.parent() // deps
                            .and_then(|d| d.parent()) // debug
                            .map(|d| d.to_owned())
                    })
                    .ok_or(BinarySourceError::NotACargoTest)?;

                let bin = test_bin_dir.join("restate-server");
                if !bin.exists() {
                    return Err(BinarySourceError::NotACargoTest);
                }

                Ok(bin.into())
            }
        }
    }
}

pub struct StartedNode {
    log_file: PathBuf,
    status: StartedNodeStatus,
    config: Configuration,
}

enum StartedNodeStatus {
    Running {
        child_handle: JoinHandle<Result<ExitStatus, io::Error>>,
        searcher: Searcher,
        pid: u32,
    },
    Exited(ExitStatus),
    Failed(ErrorKind),
}

impl Future for StartedNodeStatus {
    type Output = Result<ExitStatus, io::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut *self {
            StartedNodeStatus::Running { child_handle, .. } => match child_handle.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(Ok(status))) => {
                    *self = StartedNodeStatus::Exited(status);
                    Poll::Ready(Ok(status))
                }
                Poll::Ready(Ok(Err(err))) => {
                    *self = StartedNodeStatus::Failed(err.kind());
                    Poll::Ready(Err(err))
                }
                Poll::Ready(Err(err)) => panic!("Join error on started node task: {err}"),
            },
            StartedNodeStatus::Exited(status) => Poll::Ready(Ok(*status)),
            StartedNodeStatus::Failed(error_kind) => Poll::Ready(Err((*error_kind).into())),
        }
    }
}

impl StartedNode {
    /// Send a SIGKILL to the current process, if it is running, and await for its exit
    pub async fn kill(&mut self) -> io::Result<ExitStatus> {
        match self.status {
            StartedNodeStatus::Exited(status) => Ok(status),
            StartedNodeStatus::Failed(kind) => Err(kind.into()),
            StartedNodeStatus::Running { pid, .. } => {
                info!(
                    "Sending SIGKILL to node {} (pid {})",
                    self.config.node_name(),
                    pid
                );
                match nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid.try_into().unwrap()),
                    nix::sys::signal::SIGKILL,
                ) {
                    Ok(()) => (&mut self.status).await,
                    Err(errno) => match errno {
                        nix::errno::Errno::ESRCH => Ok(ExitStatus::default()), // ignore "no such process"
                        _ => Err(io::Error::from_raw_os_error(errno as i32)),
                    },
                }
            }
        }
    }

    /// Send a SIGTERM to the current process, if it is running
    pub fn terminate(&self) -> io::Result<()> {
        match self.status {
            StartedNodeStatus::Exited(_) => Ok(()),
            StartedNodeStatus::Failed(kind) => Err(kind.into()),
            StartedNodeStatus::Running { pid, .. } => {
                info!(
                    "Sending SIGTERM to node {} (pid {})",
                    self.config.node_name(),
                    pid
                );
                match nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid.try_into().unwrap()),
                    nix::sys::signal::SIGTERM,
                ) {
                    Err(nix::errno::Errno::ESRCH) => {
                        warn!(
                            "Node {} server process (pid {}) did not exist when sending SIGTERM",
                            self.config.node_name(),
                            pid
                        );
                        Ok(())
                    }
                    Err(errno) => Err(io::Error::from_raw_os_error(errno as i32)),
                    _ => Ok(()),
                }
            }
        }
    }

    /// Send a SIGTERM, then wait for `dur` for exit, otherwise send a SIGKILL
    pub async fn graceful_shutdown(&mut self, dur: Duration) -> io::Result<ExitStatus> {
        match self.status {
            StartedNodeStatus::Exited(status) => Ok(status),
            StartedNodeStatus::Failed(kind) => Err(kind.into()),
            StartedNodeStatus::Running { .. } => {
                let timeout = tokio::time::sleep(dur);
                let timeout = std::pin::pin!(timeout);

                self.terminate()?;

                tokio::select! {
                    () = timeout => {
                        info!(
                            "Graceful shutdown deadline exceeded for node {}",
                            self.config().node_name(),
                        );
                        self.kill().await
                    }
                    result = &mut self.status => {
                        result
                    }
                }
            }
        }
    }

    /// Get the pid of the subprocess. Returns none after it has exited.
    pub fn pid(&self) -> Option<u32> {
        match self.status {
            StartedNodeStatus::Exited { .. } | StartedNodeStatus::Failed { .. } => None,
            StartedNodeStatus::Running { pid, .. } => Some(pid),
        }
    }

    /// Wait for the node to exit and report its exist status
    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        (&mut self.status).await
    }

    pub fn config(&self) -> &Configuration {
        &self.config
    }

    pub fn node_name(&self) -> &str {
        self.config().common.node_name()
    }

    pub fn node_address(&self) -> &AdvertisedAddress {
        &self.config().common.advertised_address
    }

    pub async fn last_n_lines(&self, n: usize) -> Result<Vec<String>, rev_lines::RevLinesError> {
        let log_file = self.log_file.clone();
        tokio::task::spawn_blocking(move || {
            let log_file = std::fs::File::open(log_file)?;
            let mut lines = Vec::with_capacity(n);
            for line in RevLines::new(log_file).take(n) {
                lines.push(line?)
            }
            Ok(lines)
        })
        .await
        .unwrap_or_else(|_| Err(io::Error::other("background task failed").into()))
    }

    pub fn ingress_address(&self) -> Option<&SocketAddr> {
        if self.config().has_role(Role::Worker) {
            Some(&self.config().ingress.bind_address)
        } else {
            None
        }
    }

    pub fn admin_address(&self) -> Option<&SocketAddr> {
        if self.config().has_role(Role::Admin) {
            Some(&self.config().admin.bind_address)
        } else {
            None
        }
    }

    pub fn metadata_address(&self) -> Option<&BindAddress> {
        if self.config().has_role(Role::MetadataStore) {
            Some(&self.config().metadata_store.bind_address)
        } else {
            None
        }
    }

    /// Obtain a stream of loglines matching this pattern. The stream will end
    /// when the stdout and stderr files on the process close.
    pub fn lines(&self, pattern: Regex) -> impl Stream<Item = String> + '_ {
        match self.status {
            StartedNodeStatus::Exited { .. } => futures::stream::empty().left_stream(),
            StartedNodeStatus::Failed { .. } => futures::stream::empty().left_stream(),
            StartedNodeStatus::Running { ref searcher, .. } => {
                searcher.search(pattern).right_stream()
            }
        }
    }

    /// Obtain a metadata client based on this nodes client config.
    pub async fn metadata_client(
        &self,
    ) -> Result<restate_metadata_store::MetadataStoreClient, GenericError> {
        restate_metadata_store::local::create_client(
            self.config.common.metadata_store_client.clone(),
        )
        .await
    }

    /// Check to see if the admin address is healthy. Returns false if this node has no admin role.
    pub async fn admin_healthy(&self) -> bool {
        if let Some(address) = self.admin_address() {
            match reqwest::get(format!("http://{address}/health")).await {
                Ok(resp) => resp.status().is_success(),
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Check to see if the ingress address is healthy. Returns false if this node has no ingress role.
    pub async fn ingress_healthy(&self) -> bool {
        if let Some(address) = self.ingress_address() {
            match reqwest::get(format!("http://{address}/restate/health")).await {
                Ok(resp) => resp.status().is_success(),
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Check to see if the logserver is provisioned. Returns false if this node has no logserver role.
    pub async fn logserver_provisioned(&self) -> bool {
        let metadata_client = self
            .metadata_client()
            .await
            .expect("to get metadata client");

        let nodes_config = metadata_client
            .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
            .await;

        let Ok(Some(nodes_config)) = nodes_config else {
            return false;
        };

        let Some(node_id) = nodes_config
            .find_node_by_name(self.node_name())
            .map(|n| n.current_generation.as_plain())
        else {
            return false;
        };

        !nodes_config.get_log_server_storage_state(&node_id).empty()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum HealthCheck {
    Admin,
    Ingress,
    Logserver,
}

impl Display for HealthCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthCheck::Admin => write!(f, "admin"),
            HealthCheck::Ingress => write!(f, "ingress"),
            HealthCheck::Logserver => write!(f, "logserver"),
        }
    }
}

impl HealthCheck {
    pub fn applicable(&self, node: &StartedNode) -> bool {
        match self {
            HealthCheck::Admin => node.admin_address().is_some(),
            HealthCheck::Ingress => node.ingress_address().is_some(),
            HealthCheck::Logserver => node.config().has_role(Role::LogServer),
        }
    }

    async fn check(&self, node: &StartedNode) -> bool {
        match self {
            HealthCheck::Admin => node.admin_healthy().await,
            HealthCheck::Ingress => node.ingress_healthy().await,
            HealthCheck::Logserver => node.logserver_provisioned().await,
        }
    }

    /// Check every 250ms to see if the check is healthy, waiting for up to `timeout`.
    pub async fn wait_healthy(
        &self,
        node: &StartedNode,
        timeout: Duration,
    ) -> Result<(), HealthError> {
        if !self.applicable(node) {
            return Err(HealthError::NotApplicable(
                *self,
                node.node_name().to_owned(),
            ));
        }
        let mut attempts = 1;
        if tokio::time::timeout(timeout, async {
            while !self.check(node).await {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(250)).await
            }
        })
        .await
        .is_ok()
        {
            info!(
                "Node {} {self} check is healthy after {attempts} attempts",
                node.node_name(),
            );
            Ok(())
        } else {
            let err = HealthError::new_timeout(*self, node).await;
            error!("{err}");
            Err(err)
        }
    }
}

#[derive(Debug)]
pub enum HealthError {
    NotApplicable(HealthCheck, String),
    Timeout(HealthCheck, String, Vec<String>),
}

impl std::error::Error for HealthError {}

impl Display for HealthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotApplicable(check, node_name) => write!(
                f,
                "Check {check} is not applicable to node {node_name} and cannot pass"
            ),
            Self::Timeout(check, node_name, lines) => write!(
                f,
                r#"Timed out waiting for check {check} on node {node_name} to be active. Log tail:

NODE LOG {node_name}: {}
                "#,
                lines
                    .iter()
                    .rev()
                    .join(format!("\nNODE LOG {node_name}: ").as_str())
            ),
        }
    }
}

impl HealthError {
    async fn new_timeout(check: HealthCheck, node: &StartedNode) -> Self {
        let lines = match node.last_n_lines(20).await {
            Ok(lines) => lines,
            Err(err) => vec![format!("Failed to read loglines: {err}")],
        };
        Self::Timeout(check, node.node_name().to_owned(), lines)
    }
}

#[derive(Debug, Clone)]
pub struct Searcher {
    inner: Arc<ArcSwapOption<SearcherInner>>,
}

impl Default for Searcher {
    fn default() -> Self {
        Self {
            inner: Arc::new(ArcSwapOption::from_pointee(SearcherInner {
                set: RegexSet::empty(),
                senders: Vec::new(),
            })),
        }
    }
}

#[derive(Debug, Clone)]
struct SearcherInner {
    set: RegexSet,
    senders: Vec<Arc<Sender<String>>>,
}

impl Searcher {
    fn close(&self) {
        self.inner.rcu(|_| None);
    }

    async fn matches(&self, haystack: &str) {
        let inner = self.inner.load();
        if let Some(inner) = &*inner {
            for i in inner.set.matches(haystack) {
                // ignore sender errors; it just means we have an outdated inner and the receiver
                // has since dropped.
                _ = inner.senders[i].send(haystack.to_owned()).await;
            }
        }
    }

    fn search(&self, regex: Regex) -> impl Stream<Item = String> + 'static {
        let (sender, receiver) = mpsc::channel(1);
        let sender = Arc::new(sender);
        let sender_ptr = Arc::as_ptr(&sender);
        self.inner.rcu(|inner| {
            if let Some(inner) = inner {
                let mut patterns: Vec<String> = inner.set.patterns().into();
                patterns.push(regex.to_string());
                let mut channels = inner.senders.clone();
                channels.push(sender.clone());
                Some(Arc::new(SearcherInner {
                    set: RegexSet::new(patterns).unwrap(),
                    senders: channels,
                }))
            } else {
                // by not storing the sender, the receiver will be implicitly closed
                None
            }
        });

        Receiver {
            receiver,
            sender_ptr,
            searcher: self.inner.clone(),
        }
    }
}

pub struct Receiver {
    receiver: mpsc::Receiver<String>,
    // for comparison to the senders in the Searcher
    sender_ptr: *const Sender<String>,
    searcher: Arc<ArcSwapOption<SearcherInner>>,
}

impl Deref for Receiver {
    type Target = mpsc::Receiver<String>;

    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl DerefMut for Receiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.searcher.rcu(|inner| {
            if let Some(inner) = inner {
                let mut patterns: Vec<String> = inner.set.patterns().into();
                let mut channels = inner.senders.clone();

                let i = match channels
                    .iter()
                    .enumerate()
                    .find(|(_, s)| Arc::as_ptr(s) == self.sender_ptr)
                {
                    Some((i, _)) => i,
                    None => panic!("Could not find our pattern in the searcher"),
                };

                channels.swap_remove(i);
                patterns.swap_remove(i);

                Some(Arc::new(SearcherInner {
                    set: RegexSet::new(patterns).unwrap(),
                    senders: channels,
                }))
            } else {
                None
            }
        });
    }
}

impl Stream for Receiver {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
