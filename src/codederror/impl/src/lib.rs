//! Some parts of this codebase were took from https://github.com/dtolnay/thiserror/blob/master/impl/src
//! License APL 2.0 or MIT

extern crate proc_macro;

mod ast;
mod attr;
mod config;
mod expand;
mod generics;
mod prop;
mod valid;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

use crate::config::Config;

#[proc_macro_derive(CodedError, attributes(error, from, source, code, hint))]
pub fn derive_codederror(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand::derive(Config::from_env(), &input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
