// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod build_info;

use std::num::NonZero;
use std::path::PathBuf;

use anyhow::{Result, bail};
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use restate_core::TaskCenter;
use restate_core::TaskCenterBuilder;
use restate_core::TaskHandle;
use restate_core::TaskKind;
use restate_core::cancellation_token;
use restate_core::task_center;
use restate_node::Node;
use restate_rocksdb::RocksDbManager;
use restate_types::PlainNodeId;
use restate_types::live::Live;
use restate_types::logs::metadata::ProviderKind;
use restate_types::net::address::AdminPort;
use restate_types::net::address::AdvertisedAddress;
use restate_types::net::address::ControlPort;
use restate_types::net::address::FabricPort;
use restate_types::net::address::HttpIngressPort;
use restate_types::net::address::ListenerPort;
use restate_types::net::address::PeerNetAddress;
use restate_types::net::listener::AddressBook;
use restate_types::net::listener::Addresses;
use restate_types::nodes_config::Role;
use restate_types::replication::ReplicationProperty;

use restate_types::config::{
    BifrostOptionsBuilder, CommonOptionsBuilder, Configuration, ConfigurationBuilder, ListenMode,
    ListenerOptionsBuilder,
};

pub(crate) static RESTATE_RUNNING: Mutex<bool> = const { Mutex::const_new(false) };

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum AddressKind {
    Tcp,
    Unix,
    Http,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct AddressMeta {
    pub kind: AddressKind,
    pub name: String,
    pub address: String,
}

#[derive(Debug)]
pub struct Options {
    pub use_random_ports: bool,
    pub enable_tcp: bool,
    pub memory_budget: NonZero<usize>,
    pub data_dir: Option<PathBuf>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            memory_budget: NonZero::new(512 * 1024 * 1024).unwrap(),
            use_random_ports: false,
            enable_tcp: false,
            data_dir: None,
        }
    }
}

pub struct Restate {
    config: Configuration,
    task_center: task_center::OwnedHandle,
    task: Option<TaskHandle<Result<()>>>,
    has_stopped: Option<oneshot::Receiver<Result<()>>>,
}

impl Restate {
    pub fn get_bound_addresses(&self) -> Vec<AddressMeta> {
        let mut addresses = Vec::with_capacity(4);

        let address_book = self
            .task_center
            .address_book()
            .expect("address book is always set at creation");
        push_addresses(
            address_book.get_bound_addresses::<HttpIngressPort>(),
            &mut addresses,
        );

        push_addresses(
            address_book.get_bound_addresses::<AdminPort>(),
            &mut addresses,
        );

        push_addresses(
            address_book.get_bound_addresses::<FabricPort>(),
            &mut addresses,
        );

        push_addresses(
            address_book.get_bound_addresses::<ControlPort>(),
            &mut addresses,
        );

        addresses
    }

    pub fn get_advertised_addresses(&self) -> Vec<AddressMeta> {
        let mut addresses = Vec::with_capacity(3);
        let address_book = self
            .task_center
            .address_book()
            .expect("address book is always set at creation");

        if self.config.has_role(Role::HttpIngress) {
            push_advertised(
                self.config.ingress.advertised_address(address_book),
                &mut addresses,
            );
        }

        if self.config.has_role(Role::Admin) {
            push_advertised(
                self.config.admin.advertised_address(address_book),
                &mut addresses,
            );
        }

        // fabric
        push_advertised(
            self.config.common.advertised_address(address_book),
            &mut addresses,
        );

        addresses
    }

    pub async fn create(opts: Options) -> Result<Restate> {
        if rlimit::increase_nofile_limit(u64::MAX).is_err() {
            warn!("Failed to increase the number of open file descriptors limit.");
        }

        let listener_options = ListenerOptionsBuilder::default()
            .use_random_ports(Some(opts.use_random_ports))
            .listen_mode(Some(if opts.enable_tcp {
                ListenMode::All
            } else {
                ListenMode::Unix
            }))
            .build()
            .unwrap();

        let mut common_builder = CommonOptionsBuilder::default();

        common_builder
            .roles(Role::Worker | Role::HttpIngress | Role::MetadataServer | Role::Admin)
            .rocksdb_total_memory_size(opts.memory_budget)
            .node_name(Some("embedded".to_owned()))
            .force_node_id(Some(PlainNodeId::new(1)))
            .disable_telemetry(true)
            .fabric_listener_options(listener_options)
            .default_num_partitions(1)
            .default_replication(ReplicationProperty::new(NonZero::new(1).unwrap()))
            .disable_prometheus(true);

        if let Some(data_dir) = opts.data_dir {
            common_builder.base_dir(data_dir);
        }

        let common = common_builder.build()?;

        let bifrost = BifrostOptionsBuilder::default()
            .default_provider(ProviderKind::Local)
            .disable_auto_improvement(true)
            .build()
            .unwrap();

        let mut config = ConfigurationBuilder::default()
            .common(common)
            .bifrost(bifrost)
            .build()
            .unwrap();

        // apply config cascading propagation
        config.common.set_derived_values()?;
        config.ingress.set_derived_values(&config.common);
        config.admin.set_derived_values(&config.common);
        let config = config.apply_cascading_values();
        config.validate()?;

        let task_center = TaskCenterBuilder::default()
            .default_runtime_handle(Handle::current())
            .build()
            .unwrap();

        let mut guard = RESTATE_RUNNING.lock().await;
        if *guard {
            return Err(anyhow::anyhow!("Restate already running"));
        }

        // Setting initial configuration as global current
        restate_types::config::set_current_config(config.clone());
        // create the parent data directory if it doesn't exist
        let data_dir = restate_types::config::node_filepath("");
        if !data_dir.exists()
            && let Err(err) = std::fs::create_dir_all(&data_dir)
        {
            // We cannot use tracing here as it's not configured yet
            bail!(
                "failed to create data directory at {}: {err}",
                data_dir.display()
            );
        }

        let mut address_book = AddressBook::new(data_dir.clone());

        // Attempts to bind on all configured ports as early as possible so we can detect
        // if we can't bind to certain ports or if we can't open unix sockets before we
        // do any serious work.
        if let Err(err) = address_book.bind_from_config(&config).await {
            bail!("Failed: {err}");
        }

        let (started, has_started) = oneshot::channel();
        let (stopped, has_stopped) = oneshot::channel();
        let task = task_center.to_handle().spawn_unmanaged_child(
            TaskKind::SystemBoot,
            "restate",
            run_restate(config.clone(), data_dir, address_book, started, stopped),
        )?;

        // mark restate as running
        *guard = true;

        drop(guard);
        has_started.await?;

        Ok(Self {
            task_center,
            config,
            task: Some(task),
            has_stopped: Some(has_stopped),
        })
    }

    pub fn tokio_dump(&self) {
        let tc = self.task_center.to_handle();

        let _ = tc.spawn_unmanaged(restate_core::TaskKind::Disposable, "tokio-task-dump", {
            let tc = tc.clone();
            async move { tc.dump_tasks(std::io::stderr()).await }
        });
    }

    pub async fn discover_deployment(&self, url: &str) -> Result<()> {
        let admin_uds = self
            .get_bound_addresses()
            .iter()
            .find_map(|address| {
                if address.name == AdminPort::NAME && address.kind == AddressKind::Unix {
                    Some(address.address.clone())
                } else {
                    None
                }
            })
            .expect("admin is always set");
        // register mock service
        let client = reqwest::Client::builder().unix_socket(admin_uds).build()?;
        let discovery_payload = serde_json::json!({"uri": url.to_owned()}).to_string();
        let discovery_result = client
            .post("http://local/deployments")
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(discovery_payload)
            .send()
            .await?;

        discovery_result.error_for_status()?;
        Ok(())
    }

    pub async fn stop(mut self) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.cancel();
        }
        if let Some(has_stopped) = self.has_stopped.take() {
            let _ = has_stopped.await;
        }
        Ok(())
    }
}

impl Drop for Restate {
    fn drop(&mut self) {
        // async drop pattern
        if let Some(task) = self.task.take()
            && !task.is_finished()
            && task.is_cancellation_requested()
        {
            task.cancel();
            if let Some(has_stopped) = self.has_stopped.take()
                && let Ok(handle) = tokio::runtime::Handle::try_current()
            {
                let _ = handle.block_on(has_stopped);
            }
        }
    }
}

async fn run_restate(
    config: Configuration,
    data_dir: PathBuf,
    address_book: AddressBook,
    started: oneshot::Sender<()>,
    stopped: oneshot::Sender<Result<()>>,
) -> Result<()> {
    debug!(
        base_dir = %data_dir.display(),
        "Starting Restate {}",
        build_info::build_info()
    );

    // Initialize rocksdb manager
    let rocksdb_manager = RocksDbManager::init();

    // ensures we run rocksdb shutdown after the shutdown_node routine.
    TaskCenter::set_on_shutdown(Box::pin(async {
        rocksdb_manager.shutdown().await;
        let mut guard = RESTATE_RUNNING.lock().await;
        *guard = false;
        let _ = stopped.send(Ok(()));
        debug!("Restate stopped");
    }));

    let node = Node::create(Live::from_value(config), Default::default(), address_book).await?;
    // We ignore errors since we will wait for shutdown below anyway.
    // This starts node roles and the rest of the system async under tasks managed by
    // the TaskCenter.
    TaskCenter::spawn(TaskKind::SystemBoot, "init", async move {
        node.start().await?;
        let _ = started.send(());
        Ok(())
    })?;

    let tc_cancel_token = TaskCenter::current().shutdown_token();
    let task_cancel_token = cancellation_token();
    tokio::select! {
        () = task_cancel_token.cancelled() => {
            debug!("cancelled()");
            TaskCenter::shutdown_node("stop", 0).await;
        },
        _ = tc_cancel_token.cancelled() => {
            // Shutdown was requested by task center and it has completed.
        },
    }

    Ok(())
}

#[inline(always)]
fn push_advertised<P: ListenerPort + 'static>(
    address: AdvertisedAddress<P>,
    buf: &mut Vec<AddressMeta>,
) {
    let Ok(address) = address.into_address() else {
        return;
    };

    buf.push(AddressMeta {
        kind: match &address {
            PeerNetAddress::Uds(..) => AddressKind::Unix,
            PeerNetAddress::Http(..) => AddressKind::Http,
        },
        name: P::NAME.to_owned(),
        address: address.to_string(),
    })
}

#[inline(always)]
fn push_addresses<P: ListenerPort + 'static>(
    address: Option<Addresses<P>>,
    buf: &mut Vec<AddressMeta>,
) {
    let Some(addresses) = address else {
        return;
    };

    if let Some(tcp_bind_address) = addresses.tcp_bind_address() {
        buf.push(AddressMeta {
            name: P::NAME.to_owned(),
            address: tcp_bind_address.to_string(),
            kind: AddressKind::Tcp,
        });
    }

    if let Some(uds_path) = addresses.uds_path() {
        buf.push(AddressMeta {
            name: P::NAME.to_owned(),
            address: uds_path.display().to_string(),
            kind: AddressKind::Unix,
        });
    }
}
