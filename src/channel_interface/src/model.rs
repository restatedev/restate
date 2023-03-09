use std::collections::HashMap;
use proc_macro2::Ident;
use syn::{Attribute, braced, Generics, ImplItemConst, ImplItemMethod, ItemImpl, Signature, token, TraitItemMethod, Visibility};
use syn::parse::{Parse, ParseStream};

pub struct ChannelInterfaceDefinition {
    pub vis: Visibility,
    pub name: Ident,
    pub signatures: Vec<Signature>
}

impl Parse for ChannelInterfaceDefinition {
    fn parse(input: ParseStream) -> syn::parse::Result<Self> {
        // Parse struct definition
        let vis: Visibility = input.parse()?;
        let _struct_token: token::Struct = input.parse()?;
        let name: Ident = input.parse()?;

        // Parse block containing signatures
        let mut signatures = Vec::new();
        let block_content;
        let _brace_token = braced!(block_content in input);

        while !block_content.is_empty() {
            let item: TraitItemMethod = impl_content.parse()?;

            // Perform some validation
            if item.default.is_some() {
                return Err(input.error("Method definitions must not have an implementation block"))
            }
            if receiver.is_none() {
                return Err(input.error("Method must have &self receiver"))
            }
            if item.sig.asyncness.is_none() {
                return Err(input.error("Method must be async"))
            }

            signatures.push(item.sig);
        }

        Ok(ChannelInterfaceDefinition {
            vis,
            name,
            signatures,
        })
    }
}
