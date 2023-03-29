//! Some parts of this codebase were took from https://github.com/dtolnay/thiserror/blob/master/impl/src
//! License APL 2.0 or MIT

extern crate proc_macro;

mod ast;
mod attr;
mod expand;
mod generics;
mod prop;
mod valid;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(CodedError, attributes(error, from, source, code))]
pub fn derive_codederror(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand::derive(&input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
