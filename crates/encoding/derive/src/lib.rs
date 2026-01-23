// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate proc_macro;

mod bilrost;
mod net;
use proc_macro::TokenStream;

#[proc_macro_derive(BilrostNewType)]
pub fn bilrost_new_type(item: TokenStream) -> TokenStream {
    bilrost::new_type(item)
}

#[proc_macro_derive(BilrostAs, attributes(bilrost_as))]
pub fn bilrost_as(input: TokenStream) -> TokenStream {
    bilrost::bilrost_as(input)
}

#[proc_macro_derive(NetSerde, attributes(net_serde))]
pub fn network_message(item: TokenStream) -> TokenStream {
    net::net_serde(item)
}
