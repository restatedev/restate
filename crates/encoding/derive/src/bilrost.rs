// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input, spanned::Spanned};
use syn::{Fields, ItemStruct};

const BILROST_AS_ATTR_NAME: &str = "bilrost_as";

pub fn new_type(item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as ItemStruct);
    let inner = match &input.fields {
        Fields::Unnamed(inner) => {
            if inner.unnamed.len() != 1 {
                return syn::Error::new_spanned(
                    input.ident,
                    "This macro can only be used on newtype struct with exactly one field",
                )
                .to_compile_error()
                .into();
            }

            &inner.unnamed[0]
        }
        _ => {
            return syn::Error::new_spanned(
                input.ident,
                "This macro can only be used on newtype structs (e.g., `struct MyType(T);`)",
            )
            .to_compile_error()
            .into();
        }
    };

    let name = &input.ident;
    let inner_ty = &inner.ty;

    let output = quote! {
        #[allow(clippy::all)]
        impl<const __P: u8> ::bilrost::encoding::ValueEncoder<::bilrost::encoding::GeneralGeneric<__P>, #name> for () {
            fn encode_value<B: ::bytes::BufMut + ?Sized>(value: &#name, buf: &mut B) {
                <() as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::GeneralGeneric<__P>, #inner_ty>>::encode_value(&value.0, buf)
            }

            fn value_encoded_len(value: &#name) -> usize {
                <() as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::GeneralGeneric<__P>, #inner_ty>>::value_encoded_len(&value.0)
            }

            fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(value: &#name, buf: &mut B) {
                <() as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::GeneralGeneric<__P>, #inner_ty>>::prepend_value(&value.0, buf);
            }
        }

        #[allow(clippy::all)]
        impl<const __P: u8> ::bilrost::encoding::ValueDecoder<::bilrost::encoding::GeneralGeneric<__P>, #name> for () {
            fn decode_value<B: ::bytes::Buf + ?Sized>(
                value: &mut #name,
                buf: ::bilrost::encoding::Capped<B>,
                ctx: ::bilrost::encoding::DecodeContext,
            ) -> ::std::result::Result<(), ::bilrost::DecodeError> {
                <() as ::bilrost::encoding::ValueDecoder<::bilrost::encoding::GeneralGeneric<__P>, #inner_ty>>::decode_value(&mut value.0, buf, ctx)
            }
        }

        #[allow(clippy::all)]
        impl<const __P: u8> ::bilrost::encoding::Wiretyped<::bilrost::encoding::GeneralGeneric<__P>, #name> for () {
            const WIRE_TYPE: ::bilrost::encoding::WireType =
                <() as ::bilrost::encoding::Wiretyped<::bilrost::encoding::GeneralGeneric<__P>, #inner_ty>>::WIRE_TYPE;
        }

        #[allow(clippy::all)]
        impl ::bilrost::encoding::EmptyState<(), #name> for () {
            fn clear(val: &mut #name) {
                <() as ::bilrost::encoding::EmptyState<(), #inner_ty>>::clear(&mut val.0);
            }
            fn empty() -> #name
            where
                #name: Sized,
            {
                #name(<() as ::bilrost::encoding::EmptyState<(), #inner_ty>>::empty())
            }
            fn is_empty(val: &#name) -> bool {
                <() as ::bilrost::encoding::EmptyState<(), #inner_ty>>::is_empty(&val.0)
            }
        }

        impl ::bilrost::encoding::ForOverwrite<(), #name> for () {
            fn for_overwrite() -> #name
            where
                #name: Sized,
            {
                #name(<() as ::bilrost::encoding::ForOverwrite<(), #inner_ty>>::for_overwrite())
            }
        }
    };

    output.into()
}

pub fn bilrost_as(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let adaptor = match extract_bilrost_as_attr(&input) {
        Ok(adaptor) => adaptor,
        Err(err) => return err.to_compile_error().into(),
    };

    let name = input.ident;
    let generics = input.generics;
    let generics_inner = &generics.params;
    // We want to make our type implemented for all the General encodings so that it works in both
    // "packed" and "unpacked" contexts. The const-u8 param has to go after any lifetime generics,
    // but we can't just include a comma.
    let generics_with_general_param = if generics_inner.is_empty() {
        quote!(<const __P: u8>)
    } else {
        quote!(<#generics_inner, const __P: u8>)
    };
    let where_clause = &generics.where_clause;

    let empty_state_where_clause = match where_clause.as_ref().map(|w| &w.predicates) {
        None => {
            quote! {
                where
                    #name #generics: Default,
            }
        }
        Some(predicate) => {
            let iter = predicate.iter();
            quote! {
                where
                    #name #generics: Default,
                    #(#iter),*
            }
        }
    };

    let output = quote! {
        #[allow(clippy::all)]
        impl #generics_with_general_param ::bilrost::encoding::ValueEncoder<::bilrost::encoding::GeneralGeneric<__P>, #name #generics> for ()
        #where_clause {
            fn encode_value<B: ::bytes::BufMut + ?Sized>(value: &#name #generics, buf: &mut B) {
                let adaptor = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::create(value);
                <() as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::GeneralGeneric<__P>, #adaptor>>::encode_value(&adaptor, buf)
            }

            fn value_encoded_len(value: &#name #generics) -> usize {
                let adaptor = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::create(value);
                <() as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::GeneralGeneric<__P>, #adaptor>>::value_encoded_len(&adaptor)
            }

            fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(value: &#name #generics, buf: &mut B) {
                let adaptor = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::create(value);
                <() as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::GeneralGeneric<__P>, #adaptor>>::prepend_value(&adaptor, buf);
            }
        }

        #[allow(clippy::all)]
        impl #generics_with_general_param ::bilrost::encoding::ValueDecoder<::bilrost::encoding::GeneralGeneric<__P>, #name #generics> for ()
        #where_clause {
            fn decode_value<B: ::bytes::Buf + ?Sized>(
                value: &mut #name #generics,
                buf: ::bilrost::encoding::Capped<B>,
                ctx: ::bilrost::encoding::DecodeContext,
            ) -> ::std::result::Result<(), ::bilrost::DecodeError> {
                let mut adaptor = <() as ::bilrost::encoding::ForOverwrite<(), #adaptor>>::for_overwrite();
                <() as ::bilrost::encoding::ValueDecoder<::bilrost::encoding::GeneralGeneric<__P>, #adaptor>>::decode_value(&mut adaptor, buf, ctx)?;
                *value = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::into_inner(adaptor)?;
                Ok(())
            }
        }

        #[allow(clippy::all)]
        impl #generics_with_general_param ::bilrost::encoding::Wiretyped<::bilrost::encoding::GeneralGeneric<__P>, #name #generics> for ()
        #where_clause {
            const WIRE_TYPE: ::bilrost::encoding::WireType =
                <() as ::bilrost::encoding::Wiretyped<::bilrost::encoding::GeneralGeneric<__P>, #adaptor>>::WIRE_TYPE;
        }

        #[allow(clippy::all)]
        impl #generics ::bilrost::encoding::EmptyState<(), #name #generics> for ()
        #empty_state_where_clause {
            fn clear(val: &mut #name #generics) {
                *val = #name::default();
            }

            fn empty() -> #name #generics
            where
                #name #generics: Sized,
            {
                #name::default()
            }

            fn is_empty(val: &#name #generics) -> bool {
                let adaptor = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::create(val);
                <() as ::bilrost::encoding::EmptyState<(), #adaptor>>::is_empty(&adaptor)
            }
        }

        impl #generics ::bilrost::encoding::ForOverwrite<(), #name #generics> for ()
        #empty_state_where_clause {
            fn for_overwrite() -> #name #generics
            where
                #name #generics: Sized,
            {
                #name::default()
            }
        }
    };

    output.into()
}

fn extract_bilrost_as_attr(input: &DeriveInput) -> Result<syn::Type, syn::Error> {
    for attr in &input.attrs {
        if attr.meta.path().is_ident(BILROST_AS_ATTR_NAME) {
            return attr.parse_args();
        }
    }

    Err(syn::Error::new(
        input.span(),
        "Missing bilrost_as attribute (e.g `#[bilrost_as(TargetMessageType)]`)",
    ))
}
