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
use std::hash::{BuildHasherDefault, Hasher};
use std::net::SocketAddr;
use std::path::PathBuf;

use futures::future::OptionFuture;
use itertools::Itertools;
use listenfd::ListenFd;
use tokio::io;
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio_util::either::Either;
use tracing::{debug, info};

use crate::config::{Configuration, ListenerOptions};
use crate::nodes_config::Role;

use super::address::{AdvertisedAddress, BindAddress, ListenerPort, SocketAddress};

#[derive(Debug, thiserror::Error, codederror::CodedError)]
pub enum ListenError {
    #[error("[{service_name}] failed binding to address '{address}': {source}")]
    #[code(restate_errors::RT0004)]
    TcpBinding {
        service_name: String,
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("[{service_name}] failed binding on unix-socket file '{uds_path}': {source}")]
    #[code(restate_errors::RT0004)]
    UdsBinding {
        service_name: String,
        uds_path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[code(unknown)]
    #[error(transparent)]
    Other(#[from] std::io::Error),
}

pub struct AddressBook {
    data_dir: PathBuf,
    bound_addr: HashMap<std::any::TypeId, AddressesErased, BuildHasherDefault<IdHasher>>,
    listeners: HashMap<std::any::TypeId, ListenersErased, BuildHasherDefault<IdHasher>>,
}

impl AddressBook {
    pub const fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            listeners: HashMap::with_hasher(BuildHasherDefault::new()),
            bound_addr: HashMap::with_hasher(BuildHasherDefault::new()),
        }
    }

    /// Uses the configuration file to bind on all ports that we expect to be used.
    ///
    /// The ports are bound based on whether a role is configured or not.
    pub async fn bind_from_config(&mut self, config: &Configuration) -> Result<(), ListenError> {
        let mut listenfd = ListenFd::from_env();
        // based on the number of sockets passed via listenfd, we determine the services that will
        // use those fds. The mapping follows the order of Roles as defined in the Role enum.

        // For instance,
        // if 1, use it for ingress if ingress role is enabled (assumes that a common case is socket-activated ingress hosting)
        // if 2, use it for admin port (if admin role is enabled)
        // if 3, use it for fabric port
        // if 4, use it for nodectl port
        //  -- tokio console port cannot use listenfd listeners.

        // Note: TokioConsolePort is not bound because tokio-console does not support passing
        // in a pre-bound listener socket. Additionally, it cannot listen on both UDS and TCP
        // at the same time. This means that we will not be able to reliably get its advertised
        // address if it binds to a random port.

        let roles = config.roles().iter().sorted();
        // iterate roles in specified order as defined in [`Role`] enum
        // todo: add the nodectl port
        for role in roles {
            match role {
                Role::HttpIngress => {
                    self.bind(config.ingress.ingress_listener_options(), &mut listenfd)
                        .await?;
                }
                Role::Admin => {
                    self.bind(config.admin.admin_listener_options(), &mut listenfd)
                        .await?;
                }
                _ => {}
            }
        }

        // Message Fabric
        self.bind(config.common.fabric_listener_options(), &mut listenfd)
            .await?;
        // todo: add NodeCtl port

        Ok(())
    }

    async fn bind<P: ListenerPort>(
        &mut self,
        listener_options: &ListenerOptions<P>,
        listenfd: &mut ListenFd,
    ) -> Result<(), ListenError> {
        // Note: always attempt to listen on tcp first as we don't want to attempt deleting an
        // existing unix socket file if the tcp listener fails (in case the process is already
        // running).
        if listener_options.listen_mode().is_tcp_enabled() {
            if let Some((listener, fd)) = next_tcp_listener(listenfd)? {
                info!(
                    "[{}] Binding service to a listenfd socket listener at fd={fd} -> {}",
                    P::NAME,
                    listener.local_addr()?
                );
                self.bind_tcp_listener::<P>(listener)?;
            } else {
                let bind_address = listener_options.bind_address();
                debug!("[{}] Binding service on {bind_address}", P::NAME);
                self.bind_tcp(bind_address).await?;
            }
        }
        if listener_options.listen_mode().is_uds_enabled() {
            if P::IS_ANONYMOUS_UDS_ALLOWED
                && let Some((listener, fd)) = next_unix_listener(listenfd)?
            {
                info!(
                    "[{}] Binding service to a listenfd anonymous unix-socket at fd={fd}",
                    P::NAME,
                );
                self.bind_unix_listener::<P>(listener)?;
            } else {
                let uds_path = self.data_dir.join(P::UDS_NAME);
                debug!("[{}] Binding service on {}", P::NAME, uds_path.display());
                self.bind_uds::<P>(&uds_path).await?;
            }
        }

        Ok(())
    }

    fn bind_tcp_listener<P: ListenerPort + 'static>(
        &mut self,
        listener: TcpListener,
    ) -> Result<(), ListenError> {
        self.bound_addr
            .entry(std::any::TypeId::of::<P>())
            .or_default()
            // Use the actual bound address in case the configuration used port 0.
            // This way we capture the OS-assigned port correctly.
            .tcp_bind_address = Some(listener.local_addr()?);

        self.listeners
            .entry(std::any::TypeId::of::<P>())
            .or_default()
            .tcp_listener = Some(listener);
        Ok(())
    }

    async fn bind_tcp<P: ListenerPort + 'static>(
        &mut self,
        address: BindAddress<P>,
    ) -> Result<(), ListenError> {
        let socket_addr = address.into_inner();
        let listener =
            TcpListener::bind(socket_addr)
                .await
                .map_err(|err| ListenError::TcpBinding {
                    service_name: P::NAME.to_owned(),
                    address: socket_addr,
                    source: err,
                })?;

        self.bind_tcp_listener::<P>(listener)
    }

    fn bind_unix_listener<P: ListenerPort + 'static>(
        &mut self,
        listener: UnixListener,
    ) -> Result<(), ListenError> {
        self.bound_addr
            .entry(std::any::TypeId::of::<P>())
            .or_default()
            // this is anonymous socket
            .uds_path = None;

        self.listeners
            .entry(std::any::TypeId::of::<P>())
            .or_default()
            .unix_listener = Some(listener);
        Ok(())
    }

    async fn bind_uds<P: ListenerPort + 'static>(
        &mut self,
        uds_path: &PathBuf,
    ) -> Result<(), ListenError> {
        if uds_path.exists() {
            // if this fails, the following bind will fail, so its safe to ignore this error
            _ = std::fs::remove_file(uds_path);
        }
        // Unix socket paths have a length limit of ~108 bytes on Linux and other Unix-like
        // systems. This is a safety measure to prevent a malicious user from creating a path
        // that is too long. To try and work around this, we use a relative path to the process
        // working directory. This is not ideal, but it's the best we can do without asking the
        // user to move the entire data directory to a shorter path.
        let relative_path = {
            if let Ok(cwd) = std::env::current_dir()
                && let Ok(relative_path) = uds_path.strip_prefix(&cwd)
            {
                relative_path
            } else {
                uds_path
            }
        };
        let listener =
            UnixListener::bind(relative_path).map_err(|err| ListenError::UdsBinding {
                service_name: P::NAME.to_owned(),
                uds_path: relative_path.to_path_buf(),
                source: err,
            })?;

        self.bound_addr
            .entry(std::any::TypeId::of::<P>())
            .or_default()
            .uds_path = Some(uds_path.canonicalize()?);

        self.listeners
            .entry(std::any::TypeId::of::<P>())
            .or_default()
            .unix_listener = Some(listener);

        Ok(())
    }

    pub fn take_listeners<P: ListenerPort + 'static>(&mut self) -> Listeners<P> {
        let mut listeners = self
            .listeners
            .remove(&std::any::TypeId::of::<P>())
            .unwrap_or_default();

        Listeners {
            tcp_listener: listeners.tcp_listener.take(),
            unix_listener: listeners.unix_listener.take(),
            _phantom: std::marker::PhantomData,
        }
    }

    #[allow(dead_code)]
    pub fn get_bound_addresses<P: ListenerPort + 'static>(&self) -> Option<Addresses<P>> {
        let addresses = self.bound_addr.get(&std::any::TypeId::of::<P>()).cloned()?;

        Some(Addresses {
            tcp_bind_address: addresses.tcp_bind_address,
            uds_path: addresses.uds_path,
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn guess_advertised_address<P: ListenerPort + 'static>(
        &self,
        advertised_host: Option<&str>,
    ) -> AdvertisedAddress<P> {
        let Some(addresses) = self.bound_addr.get(&std::any::TypeId::of::<P>()) else {
            // If we don't bind this address, we return a reasonable default.
            return AdvertisedAddress::default();
        };
        // prefer TCP if available
        if let Some(tcp_address) = addresses.tcp_bind_address {
            AdvertisedAddress::derive_from_bind_address(
                SocketAddress::Socket(tcp_address),
                advertised_host,
            )
        } else if let Some(uds_path) = &addresses.uds_path {
            AdvertisedAddress::derive_from_bind_address(SocketAddress::Uds(uds_path.clone()), None)
        } else {
            // We can't guess, so we'll return a reasonable default.
            AdvertisedAddress::default()
        }
    }
}

fn next_tcp_listener(listenfd: &mut ListenFd) -> Result<Option<(TcpListener, usize)>, ListenError> {
    for i in 0..listenfd.len() {
        if let Ok(Some(listener)) = listenfd.take_tcp_listener(i) {
            listener.set_nonblocking(true)?;
            let listener = TcpListener::from_std(listener)?;
            return Ok(Some((listener, i + 3)));
        }
    }
    Ok(None)
}

fn next_unix_listener(
    listenfd: &mut ListenFd,
) -> Result<Option<(UnixListener, usize)>, ListenError> {
    for i in 0..listenfd.len() {
        if let Ok(Some(listener)) = listenfd.take_unix_listener(i) {
            listener.set_nonblocking(true)?;
            let listener = UnixListener::from_std(listener)?;
            return Ok(Some((listener, i + 3)));
        }
    }
    Ok(None)
}

#[derive(derive_more::Display)]
pub enum ListenerSocket {
    #[display("{}", listener.local_addr().unwrap())]
    Tcp {
        // bind address can be acquired from the listener
        listener: TcpListener,
    },
    #[display("unix:{}", path.display())]
    Unix {
        // bind address and listener addresses are the same
        listener: UnixListener,
        path: PathBuf,
    },
}

impl ListenerSocket {
    pub fn into_tcp_listener(self) -> Result<TcpListener, std::io::Error> {
        match self {
            ListenerSocket::Tcp { listener, .. } => Ok(listener),
            ListenerSocket::Unix { .. } => Err(std::io::Error::other(
                "cannot convert a UnixListener to a TcpListener",
            )),
        }
    }

    pub fn into_unix_listener(self) -> Result<UnixListener, std::io::Error> {
        match self {
            ListenerSocket::Tcp { .. } => Err(std::io::Error::other(
                "cannot convert a TcpListener to a UnixListener",
            )),
            ListenerSocket::Unix { listener, .. } => Ok(listener),
        }
    }

    pub fn bound_address(&self) -> SocketAddress {
        match self {
            ListenerSocket::Tcp { listener, .. } => {
                SocketAddress::Socket(listener.local_addr().unwrap())
            }
            ListenerSocket::Unix { path, .. } => SocketAddress::Uds(path.clone()),
        }
    }
}

#[derive(Default, Clone)]
struct AddressesErased {
    tcp_bind_address: Option<SocketAddr>,
    uds_path: Option<PathBuf>,
}

#[derive(Default)]
struct ListenersErased {
    tcp_listener: Option<TcpListener>,
    unix_listener: Option<UnixListener>,
}

impl Drop for ListenersErased {
    fn drop(&mut self) {
        if let Some(listener) = self.unix_listener.take()
            && let Ok(addr) = listener.local_addr()
            && let Some(path) = addr.as_pathname()
        {
            debug!("Removing unix socket at {}", path.display());
            let _ = std::fs::remove_file(path);
        }
    }
}

pub struct Addresses<P: ListenerPort> {
    tcp_bind_address: Option<SocketAddr>,
    uds_path: Option<PathBuf>,
    _phantom: std::marker::PhantomData<P>,
}

impl<P: ListenerPort + 'static> Addresses<P> {
    pub fn tcp_bind_address(&self) -> Option<SocketAddr> {
        self.tcp_bind_address
    }

    pub fn uds_path(&self) -> Option<&PathBuf> {
        self.uds_path.as_ref()
    }
}

#[derive(Default)]
pub struct Listeners<P: ListenerPort> {
    tcp_listener: Option<TcpListener>,
    unix_listener: Option<UnixListener>,
    _phantom: std::marker::PhantomData<P>,
}

impl<P: ListenerPort> Listeners<P> {
    pub fn new_unix_listener(uds_path: PathBuf) -> Result<Self, anyhow::Error> {
        let unix_listener = UnixListener::bind(uds_path)?;
        Ok(Self {
            tcp_listener: None,
            unix_listener: Some(unix_listener),
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<P: ListenerPort> Drop for Listeners<P> {
    fn drop(&mut self) {
        if let Some(listener) = self.unix_listener.take()
            && let Ok(addr) = listener.local_addr()
            && let Some(path) = addr.as_pathname()
        {
            debug!("Removing unix socket at {}", path.display());
            let _ = std::fs::remove_file(path);
        }
    }
}

impl<P: ListenerPort> Listeners<P> {
    pub fn is_valid(&self) -> bool {
        self.tcp_enabled() || self.uds_enabled()
    }

    pub fn tcp_enabled(&self) -> bool {
        self.tcp_listener.is_some()
    }

    pub fn uds_enabled(&self) -> bool {
        self.unix_listener.is_some()
    }

    pub fn uds_address(&self) -> Option<PathBuf> {
        self.unix_listener.as_ref().map(|listener| {
            listener
                .local_addr()
                .unwrap()
                .as_pathname()
                .expect("UDS path")
                .to_path_buf()
        })
    }

    pub fn tcp_address(&self) -> Option<SocketAddr> {
        self.tcp_listener
            .as_ref()
            .map(|listener| listener.local_addr().unwrap())
    }

    pub async fn accept(
        &mut self,
    ) -> Result<(Either<TcpStream, UnixStream>, SocketAddress), std::io::Error> {
        let tcp = OptionFuture::from(self.tcp_listener.as_mut().map(|t| t.accept()));
        let uds = OptionFuture::from(self.unix_listener.as_mut().map(|t| t.accept()));

        tokio::select! {
            Some(tcp_connection) = tcp => {
                let (tcp_stream, tcp_addr) = tcp_connection?;
                Ok((Either::Left(tcp_stream), SocketAddress::Socket(tcp_addr)))
            },
            Some(unix_connection) = uds => {
                let (unix_stream, unix_addr) = unix_connection?;
                Ok((Either::Right(unix_stream), SocketAddress::from_path(unix_addr.as_pathname())))
            }
            else =>  {
                panic!("No listeners are running")
            }
        }
    }
}

// With TypeIds as keys, there's no need to hash them. They are already hashes
// themselves, coming from the compiler. The IdHasher just holds the u64 of
// the TypeId, and then returns it.
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {
    fn write(&mut self, _: &[u8]) {
        unreachable!("TypeId calls write_u64");
    }

    #[inline]
    fn write_u64(&mut self, id: u64) {
        self.0 = id;
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}
