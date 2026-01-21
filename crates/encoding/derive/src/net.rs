// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Fields, Ident,
    parse::{Parse, ParseStream},
};

pub fn net_serde(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as DeriveInput);
    match net_serde_inner(input) {
        Ok(stream) => stream,
        Err(err) => err.into_compile_error().into(),
    }
}
pub fn net_serde_inner(input: DeriveInput) -> Result<TokenStream, syn::Error> {
    let name = input.ident;

    let field_types = match input.data {
        Data::Struct(data_struct) => collect_field_types(data_struct.fields)?,
        Data::Enum(data_enum) => {
            let mut field_types = HashSet::new();
            for variant in data_enum.variants {
                if skip_type(&variant.attrs)? {
                    continue;
                }
                field_types.extend(collect_field_types(variant.fields)?);
            }
            field_types
        }
        _ => panic!("NetSerde can only be derived for structs or enums"),
    };

    let where_clauses = field_types.iter().map(|ty| {
        quote! {
            #ty: NetSerde
        }
    });

    let expanded = quote! {
        impl ::restate_encoding::NetSerde for #name where #(#where_clauses),* {}
    };

    Ok(TokenStream::from(expanded))
}

fn collect_field_types(fields: Fields) -> Result<HashSet<syn::Type>, syn::Error> {
    let mut set = HashSet::new();
    match fields {
        Fields::Named(fields_named) => {
            for field in fields_named.named {
                if skip_type(&field.attrs)? {
                    continue;
                }
                set.insert(field.ty);
            }
        }
        Fields::Unnamed(fields_unnamed) => {
            for field in fields_unnamed.unnamed {
                if skip_type(&field.attrs)? {
                    continue;
                }
                set.insert(field.ty);
            }
        }
        Fields::Unit => {}
    };

    Ok(set)
}

struct NetSerdeAttrArgs {
    idents: Vec<Ident>,
}

impl Parse for NetSerdeAttrArgs {
    fn parse(input: ParseStream) -> Result<Self, syn::Error> {
        let mut idents = Vec::new();
        while !input.is_empty() {
            let ident: Ident = input.parse()?;
            idents.push(ident);
            if !input.is_empty() {
                let _ = input.parse::<syn::Token![,]>()?;
            }
        }
        Ok(NetSerdeAttrArgs { idents })
    }
}

fn skip_type(attrs: &[Attribute]) -> Result<bool, syn::Error> {
    let Some(attr) = attrs.iter().find(|attr| attr.path().is_ident("net_serde")) else {
        return Ok(false);
    };

    let args: NetSerdeAttrArgs = attr.parse_args()?;

    Ok(args.idents.iter().any(|a| a == "skip"))
}
