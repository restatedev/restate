// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Some parts copied from https://github.com/dtolnay/thiserror/blob/39aaeb00ff270a49e3c254d7b38b10e934d3c7a5/impl/src/expand.rs
//! License Apache-2.0 or MIT

use proc_macro2::TokenStream;
use quote::{ToTokens, format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Member, Result, Visibility};

use crate::ast::{Enum, Field, Input, Struct};
use crate::attr::Code;
use crate::generics::InferredBounds;

pub fn derive(node: &DeriveInput) -> Result<TokenStream> {
    let input = Input::from_syn(node)?;
    input.validate()?;
    Ok(match input {
        Input::Struct(input) => impl_struct(input),
        Input::Enum(input) => impl_enum(input),
    })
}

fn impl_struct(input: Struct) -> TokenStream {
    let ty = &input.ident;
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    let mut coded_error_inferred_bounds = InferredBounds::new();

    let code_body = if let Some(code) = input.attrs.code {
        impl_code(&code)
    } else {
        // Delegate code to field
        let code_field = input.code_field().unwrap();
        if code_field.contains_generic {
            coded_error_inferred_bounds.insert(code_field.ty, quote!(codederror::CodedError));
        }
        let member = &code_field.member;
        quote! { self.#member.code() }
    };
    let code_method = quote! {
        fn code(&self) -> std::option::Option<&'static codederror::Code> {
            #code_body
        }
    };

    let coded_error_trait = spanned_coded_error_trait(input.original);
    let coded_error_where_clause = coded_error_inferred_bounds.augment_where_clause(input.generics);
    quote! {
        #[allow(unused_variables)]
        impl #impl_generics #coded_error_trait for #ty #ty_generics #coded_error_where_clause {
            #code_method
        }
    }
}

fn impl_enum(input: Enum) -> TokenStream {
    let ty = &input.ident;
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    let mut coded_error_inferred_bounds = InferredBounds::new();

    // Generate code()
    let root_code = input.attrs.code.map(|code| impl_code(&code));
    let code_method_arms = input.variants.iter().map(|variant| {
        let ident = &variant.ident;
        let pat = fields_pat(&variant.fields);
        if let Some(ref code) = variant.attrs.code {
            let code = impl_code(code);
            quote! {
                #ty::#ident #pat => #code,
            }
        } else if let Some(code_field) = variant.code_field() {
            if code_field.contains_generic {
                coded_error_inferred_bounds.insert(code_field.ty, quote!(codederror::CodedError));
            }
            let member = member_identifier(code_field);
            quote! {
                #ty::#ident #pat => #member.code(),
            }
        } else {
            let code = root_code.clone().unwrap();
            quote! {
                #ty::#ident #pat => #code,
            }
        }
    });
    let code_method = quote! {
        fn code(&self) -> std::option::Option<&'static codederror::Code> {
           match self {
                #(#code_method_arms)*
            }
        }
    };

    let coded_error_trait = spanned_coded_error_trait(input.original);
    let coded_error_where_clause = coded_error_inferred_bounds.augment_where_clause(input.generics);
    quote! {
        #[allow(unused_variables)]
        impl #impl_generics #coded_error_trait for #ty #ty_generics #coded_error_where_clause {
            #code_method
        }
    }
}

fn fields_pat(fields: &[Field]) -> TokenStream {
    let mut members = fields.iter().map(|field| &field.member).peekable();
    match members.peek() {
        Some(Member::Named(_)) => quote!({ #(#members),* }),
        Some(Member::Unnamed(_)) => {
            let vars = members.map(|member| match member {
                Member::Unnamed(member) => format_ident!("_{}", member),
                Member::Named(_) => unreachable!(),
            });
            quote!((#(#vars),*))
        }
        None => quote!({}),
    }
}

fn member_identifier(field: &Field) -> TokenStream {
    match &field.member {
        Member::Named(n) => n.into_token_stream(),
        Member::Unnamed(i) => {
            proc_macro2::Ident::new(&format!("_{}", i.index), i.span()).into_token_stream()
        }
    }
}

fn spanned_coded_error_trait(input: &DeriveInput) -> TokenStream {
    let vis_span = match &input.vis {
        Visibility::Public(vis) => Some(vis.pub_token.span()),
        Visibility::Crate(vis) => Some(vis.crate_token.span()),
        Visibility::Restricted(vis) => Some(vis.pub_token.span()),
        Visibility::Inherited => None,
    };
    let data_span = match &input.data {
        Data::Struct(data) => data.struct_token.span(),
        Data::Enum(data) => data.enum_token.span(),
        Data::Union(data) => data.union_token.span(),
    };
    let first_span = vis_span.unwrap_or(data_span);
    let last_span = input.ident.span();
    let path = quote_spanned!(first_span=> codederror::);
    let error = quote_spanned!(last_span=> CodedError);
    quote!(#path #error)
}

fn impl_code(code: &Code) -> TokenStream {
    if let Some(ref code_ident) = code.value {
        quote! {
            std::option::Option::Some(
                &#code_ident
            )
        }
    } else {
        quote! {
            std::option::Option::None
        }
    }
}
