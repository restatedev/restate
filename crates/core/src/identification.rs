// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// re-export the AddressKind enum from the protobuf
pub use crate::protobuf::node_ctl_svc::ident_response::AddressKind as NetAddressKind;

use std::time::Duration;

use enumset::EnumSet;
use serde_with::serde_as;

use restate_time_util::FriendlyDuration;
use restate_types::config::Configuration;
use restate_types::health::{
    AdminStatus, LogServerStatus, MetadataServerStatus, NodeStatus, WorkerStatus,
};
use restate_types::net::address::{
    AdminPort, AdvertisedAddress, ControlPort, FabricPort, HttpIngressPort, ListenerPort,
    PeerNetAddress,
};
use restate_types::net::listener::{AddressBook, Addresses};
use restate_types::nodes_config::Role;
use restate_types::{NodeId, Version};

use crate::task_center::TaskCenterMonitoring;
use crate::{Metadata, TaskCenter};

#[serde_as]
#[derive(serde::Serialize, prost_dto::IntoProst)]
#[prost(target = "crate::protobuf::node_ctl_svc::IdentResponse")]
pub struct Identification {
    pub status: NodeStatus,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub node_id: Option<NodeId>,
    pub cluster_name: String,
    #[into_prost(map = "enum_set_to_vec", map_by_ref)]
    pub roles: EnumSet<Role>,
    #[prost(name=age_s)]
    #[into_prost(map=Duration::as_secs, map_by_ref)]
    pub age: FriendlyDuration,
    pub admin_status: AdminStatus,
    pub worker_status: WorkerStatus,
    pub log_server_status: LogServerStatus,
    pub metadata_server_status: MetadataServerStatus,
    pub nodes_config_version: Version,
    pub logs_version: Version,
    pub schema_version: Version,
    pub partition_table_version: Version,
    pub bound_addresses: Vec<NetAddress>,
    pub advertised_addresses: Vec<NetAddress>,
}

#[derive(serde::Serialize, prost_dto::IntoProst)]
#[prost(target = "crate::protobuf::node_ctl_svc::ident_response::NetAddress")]
pub struct NetAddress {
    pub name: String,
    pub address: String,
    pub kind: NetAddressKind,
}

fn enum_set_to_vec(roles: &EnumSet<Role>) -> Vec<String> {
    roles.iter().map(|role| role.to_string()).collect()
}

impl Identification {
    /// Gets the identification information for this node. It needs to be called from within the
    /// [`TaskCenter`].
    pub fn get() -> Self {
        let configuration = Configuration::pinned();

        let (
            node_status,
            admin_status,
            worker_status,
            metadata_server_status,
            log_server_status,
            bound_addresses,
            advertised_addresses,
        ) = TaskCenter::with_current(|tc| {
            let health = tc.health();
            let address_book = tc.address_book();

            let bound_addresses = collect_bound_addresses(address_book);
            let advertised_addresses = collect_advertised_addresses(&configuration, address_book);
            (
                health.current_node_status(),
                health.current_admin_status(),
                health.current_worker_status(),
                health.current_metadata_store_status(),
                health.current_log_server_status(),
                bound_addresses,
                advertised_addresses,
            )
        });
        let age = TaskCenter::with_current(|tc| tc.age());
        let metadata = Metadata::current();

        Identification {
            status: node_status,
            node_id: metadata.my_node_id_opt().map(Into::into),
            roles: *configuration.roles(),
            cluster_name: configuration.common.cluster_name().to_owned(),
            age: age.into(),
            admin_status,
            worker_status,
            metadata_server_status,
            log_server_status,
            nodes_config_version: metadata.nodes_config_version(),
            logs_version: metadata.logs_version(),
            schema_version: metadata.schema_version(),
            partition_table_version: metadata.partition_table_version(),
            bound_addresses,
            advertised_addresses,
        }
    }
}

fn collect_bound_addresses(address_book: &AddressBook) -> Vec<NetAddress> {
    let mut addresses = Vec::with_capacity(4);

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

fn collect_advertised_addresses(
    config: &Configuration,
    address_book: &AddressBook,
) -> Vec<NetAddress> {
    let mut addresses = Vec::with_capacity(3);

    if config.has_role(Role::HttpIngress) {
        push_advertised(
            config.ingress.advertised_address(address_book),
            &mut addresses,
        );
    }

    if config.has_role(Role::Admin) {
        push_advertised(
            config.admin.advertised_address(address_book),
            &mut addresses,
        );
    }

    // fabric
    push_advertised(
        config.common.advertised_address(address_book),
        &mut addresses,
    );

    addresses
}

#[inline(always)]
fn push_addresses<P: ListenerPort + 'static>(
    address: Option<Addresses<P>>,
    buf: &mut Vec<NetAddress>,
) {
    let Some(addresses) = address else {
        return;
    };

    if let Some(tcp_bind_address) = addresses.tcp_bind_address() {
        buf.push(NetAddress {
            name: P::NAME.to_owned(),
            address: tcp_bind_address.to_string(),
            kind: NetAddressKind::Tcp,
        });
    }

    if let Some(uds_path) = addresses.uds_path() {
        buf.push(NetAddress {
            name: P::NAME.to_owned(),
            address: uds_path.display().to_string(),
            kind: NetAddressKind::Unix,
        });
    }
}

#[inline(always)]
fn push_advertised<P: ListenerPort + 'static>(
    address: AdvertisedAddress<P>,
    buf: &mut Vec<NetAddress>,
) {
    let Ok(address) = address.into_address() else {
        return;
    };

    buf.push(NetAddress {
        kind: match &address {
            PeerNetAddress::Uds(..) => NetAddressKind::Unix,
            PeerNetAddress::Http(..) => NetAddressKind::Http,
        },
        name: P::NAME.to_owned(),
        address: address.to_string(),
    })
}
