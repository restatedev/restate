// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Some parts of this codebase were taken from https://github.com/dtolnay/thiserror/tree/39aaeb00ff270a49e3c254d7b38b10e934d3c7a5/impl/src
//! License Apache-2.0 or MIT

extern crate proc_macro;

mod ast;
mod attr;
mod expand;
mod generics;
mod prop;
mod valid;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(CodedError, attributes(error, from, source, code))]
pub fn derive_codederror(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand::derive(&input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
