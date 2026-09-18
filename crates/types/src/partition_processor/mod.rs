// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Client side abstractions to talk to the partition processor owning a given partition key.
//!
//! A partition processor client is a a client that takes a [`PartitionProcessorRpc`], and returns a [`PartitionProcessorRpc::Response`]. Each RPC is a dedicated request/response message pair.
//! The lifecycle of a request is roughly the following:
//!
//!   1. The client resolves the partition key of the request.
//!   2. Using the partition routing table, the client identifies the corresponding partition id for that key.
//!   3. Establishs a connetion with the leader of that partition.
//!   4. Serializes the request into its wire format using [`PartitionProcessorRpc::into_wire`].
//!   5. Sends the RPC to the partition processor.
//!   6. When the response arrives, it converts the response wire format to the response via [`PartitionProcessorRpc::from_wire`].
//!   7. Ships the response back to the client.
//!
//! To add a new RPC, define its Request/Response types in this module, and their corresponding wire formats in (net/partition_processor.rs). Then register them
//! as RPCs via the [`define_partition_processor_rpc`] macro.
//!
//!
//! TODO: Remove in 1.9
//! Some existing RPCs used to be multiplexed onto one fat RPC ([`PartitionProcessorRpcRequest`]). So for the duration of the transition,
//! those are RPCs can be (de)serialized from and to the wire format of the fat RPC as well via [`PartitionProcessorRpc::into_legacy_wire`] and [`PartitionProcessorRpc::from_legacy_wire`].
//! It's up to the client implementation to define when to use the legacy versus the dedicated wire format.

pub mod client;
pub mod requests;
