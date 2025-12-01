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
    num::NonZeroU16,
    ops::{Deref, DerefMut},
    path::PathBuf,
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::bail;
use arc_swap::ArcSwapOption;
use enumset::EnumSet;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use rand::seq::IteratorRandom;
use regex::{Regex, RegexSet};
use rev_lines::RevLines;
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::mpsc,
    sync::mpsc::Sender,
    task::JoinHandle,
};
use tonic::Code;
use tracing::{error, info, warn};
use typed_builder::TypedBuilder;

use restate_core::network::net_util::{DNSResolution, create_tonic_channel};
use restate_core::protobuf::node_ctl_svc::{
    ProvisionClusterRequest as ProtoProvisionClusterRequest, new_node_ctl_client,
};
use restate_metadata_server_grpc::grpc::{
    RemoveNodeRequest, StatusResponse, new_metadata_server_client,
};
use restate_metadata_store::ReadError;
use restate_types::config::InvalidConfigurationError;
use restate_types::logs::metadata::ProviderConfiguration;
use restate_types::net::address::{
    AdminPort, AdvertisedAddress, FabricPort, HttpIngressPort, ListenerPort, PeerNetAddress,
};
use restate_types::nodes_config::MetadataServerState;
use restate_types::protobuf::common::MetadataServerStatus;
use restate_types::replication::ReplicationProperty;
use restate_types::retries::RetryPolicy;
use restate_types::{
    PlainNodeId,
    config::{Configuration, MetadataClientKind},
    errors::GenericError,
    metadata_store::keys::NODES_CONFIG_KEY,
    nodes_config::{NodesConfiguration, Role},
};

#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
pub struct NodeSpec {
    #[builder(mutators(
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
    #[error(transparent)]
    DeriveBindAddress(#[from] InvalidConfigurationError),
    #[error("Failed to dump config to bytes: {0}")]
    DumpConfig(GenericError),
    #[error("Failed to spawn restate-server: {0}")]
    SpawnError(io::Error),
}

impl NodeSpec {
    pub fn node_name(&self) -> &str {
        self.base_config.node_name()
    }

    pub fn metadata_store_client_mut(&mut self) -> &mut MetadataClientKind {
        &mut self.base_config.common.metadata_client.kind
    }

    pub fn config(&self) -> &Configuration {
        &self.base_config
    }

    pub fn config_mut(&mut self) -> &mut Configuration {
        &mut self.base_config
    }

    /// Creates a test node
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
            .with_roles(roles)
            .build()
    }

    /// Creates a set of [`Node`] that all run the [`Role::Admin`] and [`Role::MetadataServer`]
    /// roles and the embedded metadata store. Additionally, they will run the provided set of
    /// roles. Node name, roles, bind/advertise addresses, and the metadata address from
    /// the base_config will all be overwritten.
    pub fn new_test_nodes(
        mut base_config: Configuration,
        binary_source: BinarySource,
        roles: EnumSet<Role>,
        size: u32,
        auto_provision: bool,
    ) -> Vec<Self> {
        let mut nodes = Vec::with_capacity(usize::try_from(size).expect("u32 to fit into usize"));

        base_config.common.auto_provision = false;
        base_config.common.log_disable_ansi_codes = true;

        for node_id in 1..=size {
            let mut effective_config = base_config.clone();
            effective_config.common.force_node_id = Some(PlainNodeId::new(node_id));

            if auto_provision && node_id == 1 {
                // the first node will be responsible for bootstrapping the cluster
                effective_config.common.auto_provision = true;
            }

            let node = Self::new_test_node(
                format!("node-{node_id}"),
                effective_config,
                binary_source.clone(),
                roles,
            );

            nodes.push(node);
        }

        nodes
    }

    pub fn set_metadata_servers(&mut self, all_servers: &[AdvertisedAddress<FabricPort>]) {
        if let MetadataClientKind::Replicated { addresses } =
            &mut self.base_config.common.metadata_client.kind
        {
            addresses.extend_from_slice(all_servers);
        }
    }

    /// Start this Node, providing the base_dir and the cluster_name of the cluster it's
    /// expected to attach to. All relative file paths addresses specified in the node config
    /// (eg, nodename/node.sock) will be absolutized against the base path, and the base dir
    /// and cluster name present in config will be overwritten.
    pub async fn start_clustered(
        mut self,
        base_dir: impl Into<PathBuf>,
        cluster_name: impl Into<String>,
    ) -> Result<StartedNode, NodeStartError> {
        let base_dir = base_dir.into();

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
        } = &self;

        let node_base_dir = std::path::absolute(
            base_config
                .common
                .base_dir()
                .join(base_config.common.node_name()),
        )
        .map_err(NodeStartError::Absolute)?;

        // set advertised addresses to make it easier to address this node from the test harness.
        // todo: add tcp support
        let fabric_advertised_address = AdvertisedAddress::with_node_base_dir(&node_base_dir);
        let ingress_advertised_address = base_config
            .has_role(Role::HttpIngress)
            .then_some(AdvertisedAddress::with_node_base_dir(&node_base_dir));
        let admin_advertised_address = base_config
            .has_role(Role::Admin)
            .then_some(AdvertisedAddress::with_node_base_dir(&node_base_dir));

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
            config_file
                .flush()
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

        let binary_path: OsString = binary_source.clone().try_into()?;
        let mut cmd = Command::new(&binary_path);

        if !inherit_env {
            cmd.env_clear()
        } else {
            &mut cmd
        }
        .env("RESTATE_CONFIG", node_config_file)
        .env("DO_NOT_TRACK", "true") // avoid sending telemetry as part of tests
        .envs(env.clone())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0) // avoid terminal control C being propagated
        .args(args);

        let mut child = cmd.spawn().map_err(NodeStartError::SpawnError)?;
        let pid = child.id().expect("child to have a pid");

        info!(
            %fabric_advertised_address,
            admin_advertised_address = ?admin_advertised_address,
            ingress_advertised_address = ?ingress_advertised_address,
            "Started node {} in {} (pid {pid})",
            base_config.node_name(),
            node_base_dir.display(),
        );
        let stdout = child.stdout.take().expect("child to have a stdout pipe");
        let stderr = child.stderr.take().expect("child to have a stderr pipe");

        if self.config().has_role(Role::Admin) {
            info!(
                "To connect to node {} using restate CLI:\nexport RESTATE_ADMIN_URL={}",
                base_config.node_name(),
                admin_advertised_address.as_ref().unwrap(),
            );
        }

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
            fabric_advertised_address,
            admin_advertised_address,
            ingress_advertised_address,
            status: StartedNodeStatus::Running {
                child_handle,
                searcher: searcher.clone(),
                pid,
            },
            node: Some(self),
        })
    }

    /// Obtain a stream of loglines matching this pattern. The stream will end
    /// when the stdout and stderr files on the process close.
    pub fn lines(&self, pattern: Regex) -> impl Stream<Item = String> + 'static {
        self.searcher.search(pattern)
    }

    pub fn has_role(&self, role: Role) -> bool {
        self.base_config.roles().contains(role)
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
    fabric_advertised_address: AdvertisedAddress<FabricPort>,
    admin_advertised_address: Option<AdvertisedAddress<AdminPort>>,
    ingress_advertised_address: Option<AdvertisedAddress<HttpIngressPort>>,
    status: StartedNodeStatus,
    node: Option<NodeSpec>,
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
    pub fn has_role(&self, role: Role) -> bool {
        self.config().has_role(role)
    }

    /// Send a SIGKILL to the current process, if it is running, and await for its exit
    pub async fn kill(&mut self) -> io::Result<ExitStatus> {
        match self.status {
            StartedNodeStatus::Exited(status) => Ok(status),
            StartedNodeStatus::Failed(kind) => Err(kind.into()),
            StartedNodeStatus::Running { pid, .. } => {
                info!("Sending SIGKILL to node {} (pid {})", self.node_name(), pid);
                match nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid.try_into().expect("pid_t = i32")),
                    nix::sys::signal::SIGKILL,
                ) {
                    Ok(()) => (&mut self.status).await,
                    Err(errno) => match errno {
                        nix::errno::Errno::ESRCH => {
                            self.status = StartedNodeStatus::Exited(ExitStatus::default());
                            Ok(ExitStatus::default())
                        } // ignore "no such process"
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
                info!("Sending SIGTERM to node {} (pid {})", self.node_name(), pid);
                match nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid.try_into().expect("pid_t = i32")),
                    nix::sys::signal::SIGTERM,
                ) {
                    Err(nix::errno::Errno::ESRCH) => {
                        warn!(
                            "Node {} server process (pid {}) did not exist when sending SIGTERM",
                            self.node_name(),
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

    pub async fn restart(&mut self, termination_signal: TerminationSignal) -> anyhow::Result<()> {
        info!("Restarting node '{}'", self.config().node_name());

        match termination_signal {
            TerminationSignal::SIGKILL => {
                self.kill().await?;
            }
            TerminationSignal::SIGTERM => {
                self.terminate()?;
                (&mut self.status).await?;
            }
        }

        assert!(
            !matches!(self.status, StartedNodeStatus::Running { .. }),
            "Node should not be in status running after killing it."
        );
        *self = self.node.take().expect("to be present").start().await?;
        Ok(())
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
        &self.node.as_ref().expect("to be present").base_config
    }

    pub fn node_name(&self) -> &str {
        self.config().common.node_name()
    }

    pub fn advertised_address(&self) -> &AdvertisedAddress<FabricPort> {
        &self.fabric_advertised_address
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

    pub fn ingress_address(&self) -> &Option<AdvertisedAddress<HttpIngressPort>> {
        &self.ingress_advertised_address
    }

    pub fn admin_address(&self) -> &Option<AdvertisedAddress<AdminPort>> {
        &self.admin_advertised_address
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
    ) -> anyhow::Result<restate_metadata_store::MetadataStoreClient> {
        restate_metadata_providers::create_client(self.config().common.metadata_client.clone())
            .await
    }

    async fn probe_health<P: ListenerPort>(
        address: &AdvertisedAddress<P>,
        relative_url: &str,
    ) -> bool {
        let address = match address.clone().into_address() {
            Ok(address) => address,
            Err(err) => {
                // if it's a unix socket, we might not have created the file yet
                warn!("Error in address: {err}");
                return false;
            }
        };

        let result = match address {
            PeerNetAddress::Uds(socket_path) => {
                reqwest::Client::builder()
                    .unix_socket(socket_path)
                    .build()
                    .unwrap()
                    .get(format!("http://local/{relative_url}"))
                    .header(http::header::ACCEPT, "application/json")
                    .send()
                    .await
            }

            PeerNetAddress::Http(base_url) => {
                reqwest::Client::builder()
                    .build()
                    .unwrap()
                    .get(format!("{base_url}/{relative_url}"))
                    .header(http::header::ACCEPT, "application/json")
                    .send()
                    .await
            }
        };

        match result {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    /// Check to see if the admin address is healthy. Returns false if this node has no admin role.
    pub async fn admin_healthy(&self) -> bool {
        let Some(address) = self.admin_address() else {
            return false;
        };
        Self::probe_health(address, "health").await
    }

    /// Check to see if the ingress address is healthy. Returns false if this node has no ingress role.
    pub async fn ingress_healthy(&self) -> bool {
        let Some(address) = self.ingress_address() else {
            return false;
        };
        Self::probe_health(address, "restate/health").await
    }

    /// Check to see if the logserver is provisioned.
    pub async fn logserver_provisioned(&self) -> bool {
        let nodes_config = self.get_nodes_configuration().await;

        let Ok(Some(nodes_config)) = nodes_config else {
            return false;
        };

        let Some(node_id) = nodes_config
            .find_node_by_name(self.node_name())
            .map(|n| n.current_generation.as_plain())
        else {
            return false;
        };

        !nodes_config
            .get_log_server_storage_state(&node_id)
            .is_provisioning()
    }

    /// Check to see if the worker is provisioned.
    pub async fn worker_provisioned(&self) -> bool {
        let nodes_config = self.get_nodes_configuration().await;

        let Ok(Some(nodes_config)) = nodes_config else {
            return false;
        };

        let Some(node_id) = nodes_config
            .find_node_by_name(self.node_name())
            .map(|n| n.current_generation.as_plain())
        else {
            return false;
        };

        !nodes_config.get_worker_state(&node_id).is_provisioning()
    }

    async fn get_nodes_configuration(&self) -> Result<Option<NodesConfiguration>, ReadError> {
        let metadata_client = self
            .metadata_client()
            .await
            .expect("to get metadata client");

        metadata_client
            .get::<NodesConfiguration>(NODES_CONFIG_KEY.clone())
            .await
    }

    /// Check to see if the metadata server has joined the metadata cluster if its
    /// metadata server state is [`MetadataServerState::Member`].
    pub async fn metadata_server_joined_cluster(&self) -> bool {
        let nodes_configuration = self.get_nodes_configuration().await;

        // if we can't obtain the `NodesConfiguration`, then our cluster has not been provisioned yet
        let Ok(Some(nodes_config)) = nodes_configuration else {
            return false;
        };

        let Some(node_config) = nodes_config.find_node_by_name(self.node_name()) else {
            return false;
        };

        if node_config.metadata_server_config.metadata_server_state == MetadataServerState::Standby
        {
            return true;
        }

        let mut metadata_server_client = new_metadata_server_client(create_tonic_channel(
            self.fabric_advertised_address.clone(),
            &self.config().networking,
            DNSResolution::Gai,
        ));

        let Ok(response) = metadata_server_client
            .status(())
            .await
            .map(|response| response.into_inner())
        else {
            return false;
        };

        response.status() == MetadataServerStatus::Member
    }

    /// Provisions the cluster on this node with the given configuration. Returns true if the
    /// cluster was newly provisioned.
    pub async fn provision_cluster(
        &self,
        num_partitions: Option<NonZeroU16>,
        partition_replication: ReplicationProperty,
        provider_configuration: Option<ProviderConfiguration>,
    ) -> anyhow::Result<bool> {
        let channel = create_tonic_channel(
            self.advertised_address().clone(),
            &Configuration::default().networking,
            DNSResolution::Gai,
        );

        let request = ProtoProvisionClusterRequest {
            dry_run: false,
            num_partitions: num_partitions.map(|num| u32::from(num.get())),
            partition_replication: Some(partition_replication.into()),
            log_provider: provider_configuration
                .as_ref()
                .map(|config| config.kind().to_string()),
            log_replication: provider_configuration
                .as_ref()
                .and_then(|config| config.replication().cloned())
                .map(Into::into),
            target_nodeset_size: provider_configuration.as_ref().and_then(|config| {
                config
                    .target_nodeset_size()
                    .map(|nodeset_size| nodeset_size.as_u32())
            }),
        };

        let retry_policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            Some(60), // our test infra is sometimes slow until the node starts up
            Some(Duration::from_secs(1)),
        );
        let client = new_node_ctl_client(channel);

        let response = retry_policy
            .retry(|| {
                let mut client = client.clone();
                let request = request.clone();
                async move { client.provision_cluster(request).await }
            })
            .await;

        match response {
            Ok(response) => {
                let response = response.into_inner();

                assert!(!response.dry_run, "provision command was run w/o dry run");
                Ok(true)
            }
            Err(status) => {
                if status.code() == Code::AlreadyExists {
                    Ok(false)
                } else {
                    bail!(
                        "failed to provision the cluster at node {}: {status}",
                        self.advertised_address()
                    );
                }
            }
        }
    }

    pub async fn add_as_metadata_member(&self) -> anyhow::Result<()> {
        let mut client = new_metadata_server_client(create_tonic_channel(
            self.advertised_address().clone(),
            &self.config().networking,
            DNSResolution::Gai,
        ));

        client.add_node(()).await?;

        Ok(())
    }

    pub async fn remove_metadata_member(&self, node_to_remove: PlainNodeId) -> anyhow::Result<()> {
        let mut client = new_metadata_server_client(create_tonic_channel(
            self.advertised_address().clone(),
            &self.config().networking,
            DNSResolution::Gai,
        ));

        client
            .remove_node(RemoveNodeRequest {
                plain_node_id: u32::from(node_to_remove),
                created_at_millis: None,
            })
            .await?;

        Ok(())
    }

    pub async fn get_metadata_server_status(&self) -> anyhow::Result<StatusResponse> {
        let mut client = new_metadata_server_client(create_tonic_channel(
            self.advertised_address().clone(),
            &self.config().networking,
            DNSResolution::Gai,
        ));
        let response = client.status(()).await?.into_inner();
        Ok(response)
    }
}

/// How to terminate a running Restate process
#[derive(Debug, Clone, Copy, strum::EnumIter)]
pub enum TerminationSignal {
    SIGKILL,
    SIGTERM,
}

impl TerminationSignal {
    pub fn random() -> Self {
        Self::iter().choose(&mut rand::rng()).unwrap()
    }
}

impl Drop for StartedNode {
    fn drop(&mut self) {
        if let StartedNodeStatus::Running { pid, .. } = self.status {
            warn!(
                "Node {} (pid {}) dropped without explicit shutdown",
                self.config().node_name(),
                pid,
            );
            match nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid.try_into().expect("pid_t = i32")),
                nix::sys::signal::SIGKILL,
            ) {
                Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
                err => error!(
                    "Failed to send SIGKILL to running node {} (pid {}): {:?}",
                    self.config().node_name(),
                    pid,
                    err,
                ),
            }
        }
    }
}

#[derive(Debug, Clone, Copy, derive_more::Display)]
pub enum HealthCheck {
    Admin,
    Ingress,
    Worker,
    LogServer,
    MetadataServer,
}

impl HealthCheck {
    pub fn applicable(&self, node: &StartedNode) -> bool {
        match self {
            HealthCheck::Admin => node.admin_address().is_some(),
            HealthCheck::Ingress => node.ingress_address().is_some(),
            HealthCheck::Worker => node.config().has_role(Role::Worker),
            HealthCheck::LogServer => node.config().has_role(Role::LogServer),
            HealthCheck::MetadataServer => node.config().has_role(Role::MetadataServer),
        }
    }

    async fn check(&self, node: &StartedNode) -> bool {
        match self {
            HealthCheck::Admin => node.admin_healthy().await,
            HealthCheck::Ingress => node.ingress_healthy().await,
            HealthCheck::LogServer => node.logserver_provisioned().await,
            HealthCheck::Worker => node.worker_provisioned().await,
            HealthCheck::MetadataServer => node.metadata_server_joined_cluster().await,
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
