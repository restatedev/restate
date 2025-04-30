// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use syn::{Data, DeriveInput, Fields};

pub fn net_serde(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as DeriveInput);

    let name = input.ident;

    let field_types = match input.data {
        Data::Struct(data_struct) => collect_field_types(data_struct.fields),
        Data::Enum(data_enum) => {
            let mut field_types = HashSet::new();
            for variant in data_enum.variants {
                field_types.extend(collect_field_types(variant.fields));
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

    TokenStream::from(expanded)
}

fn collect_field_types(fields: Fields) -> HashSet<syn::Type> {
    match fields {
        Fields::Named(fields_named) => fields_named.named.into_iter().map(|f| f.ty).collect(),
        Fields::Unnamed(fields_unnamed) => {
            fields_unnamed.unnamed.into_iter().map(|f| f.ty).collect()
        }
        Fields::Unit => HashSet::default(),
    }
}
