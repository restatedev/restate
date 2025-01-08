// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tonic::transport::Channel;

use restate_cli_util::CliContext;
use restate_core::network::net_util::create_tonic_channel;
use restate_types::net::AdvertisedAddress;

pub fn grpc_connect(address: AdvertisedAddress) -> Channel {
    let ctx = CliContext::get();
    create_tonic_channel(address, &ctx.network)
}
