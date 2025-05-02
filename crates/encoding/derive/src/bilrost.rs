// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
        impl ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General> for #name {
            fn encode_value<B: ::bytes::BufMut + ?Sized>(value: &Self, buf: &mut B) {
                <#inner_ty as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General>>::encode_value(&value.0, buf)
            }

            fn value_encoded_len(value: &Self) -> usize {
                <#inner_ty as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General>>::value_encoded_len(&value.0)
            }

            fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(value: &Self, buf: &mut B) {
                <#inner_ty as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General>>::prepend_value(&value.0, buf);
            }
        }

        #[allow(clippy::all)]
        impl ::bilrost::encoding::ValueDecoder<::bilrost::encoding::General> for #name {
            fn decode_value<B: ::bytes::Buf + ?Sized>(
                value: &mut Self,
                buf: ::bilrost::encoding::Capped<B>,
                ctx: ::bilrost::encoding::DecodeContext,
            ) -> ::std::result::Result<(), ::bilrost::DecodeError> {
                <#inner_ty as ::bilrost::encoding::ValueDecoder<::bilrost::encoding::General>>::decode_value(&mut value.0, buf, ctx)
            }
        }

        #[allow(clippy::all)]
        impl ::bilrost::encoding::Wiretyped<::bilrost::encoding::General> for #name {
            const WIRE_TYPE: ::bilrost::encoding::WireType = <#inner_ty as ::bilrost::encoding::Wiretyped<::bilrost::encoding::General>>::WIRE_TYPE;
        }

        #[allow(clippy::all)]
        impl ::bilrost::encoding::EmptyState for #name {
            fn clear(&mut self) {
                <#inner_ty as ::bilrost::encoding::EmptyState>::clear(&mut self.0);
            }
            fn empty() -> Self
            where
                Self: Sized,
            {
                Self(<#inner_ty as ::bilrost::encoding::EmptyState>::empty())
            }
            fn is_empty(&self) -> bool {
                <#inner_ty as ::bilrost::encoding::EmptyState>::is_empty(&self.0)
            }
        }

        impl ::bilrost::encoding::ForOverwrite for #name {
            fn for_overwrite() -> Self
            where
                Self: Sized,
            {
                Self(<#inner_ty as ::bilrost::encoding::ForOverwrite>::for_overwrite())
            }
        }
    };

    output.into()
}

pub fn bilrost_as(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let adaptor = match extract_bilorst_as_attr(&input) {
        Ok(adaptor) => adaptor,
        Err(err) => return err.to_compile_error().into(),
    };

    let name = &input.ident;

    let output = quote! {
        #[allow(clippy::all)]
        impl ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General> for #name {
            fn encode_value<B: ::bytes::BufMut + ?Sized>(value: &Self, buf: &mut B) {
                let adaptor = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::create(value);
                <#adaptor as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General>>::encode_value(&adaptor, buf)
            }

            fn value_encoded_len(value: &Self) -> usize {
                let adaptor = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::create(value);
                <#adaptor as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General>>::value_encoded_len(&adaptor)
            }

            fn prepend_value<B: bilrost::buf::ReverseBuf + ?Sized>(value: &Self, buf: &mut B) {
                let adaptor = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::create(value);
                <#adaptor as ::bilrost::encoding::ValueEncoder<::bilrost::encoding::General>>::prepend_value(&adaptor, buf);
            }
        }

        #[allow(clippy::all)]
        impl ::bilrost::encoding::ValueDecoder<::bilrost::encoding::General> for #name {
            fn decode_value<B: ::bytes::Buf + ?Sized>(
                value: &mut Self,
                buf: ::bilrost::encoding::Capped<B>,
                ctx: ::bilrost::encoding::DecodeContext,
            ) -> ::std::result::Result<(), ::bilrost::DecodeError> {
                let mut adaptor = <#adaptor as ::bilrost::encoding::ForOverwrite>::for_overwrite();
                <#adaptor as ::bilrost::encoding::ValueDecoder<::bilrost::encoding::General>>::decode_value(&mut adaptor, buf, ctx)?;
                *value = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::into_inner(adaptor)?;
                Ok(())
            }
        }

        #[allow(clippy::all)]
        impl ::bilrost::encoding::Wiretyped<::bilrost::encoding::General> for #name {
            const WIRE_TYPE: ::bilrost::encoding::WireType = <#adaptor as ::bilrost::encoding::Wiretyped<::bilrost::encoding::General>>::WIRE_TYPE;
        }

        #[allow(clippy::all)]
        impl ::bilrost::encoding::EmptyState for #name where #name: Default {
            fn clear(&mut self) {
                *self = Self::empty();
            }
            fn empty() -> Self
            where
                Self: Sized,
            {
                #name::default()
            }

            fn is_empty(&self) -> bool {
                let adaptor = <#adaptor as ::restate_encoding::BilrostAsAdaptor<_>>::create(self);
                <#adaptor as ::bilrost::encoding::EmptyState>::is_empty(&adaptor)
            }
        }

        impl ::bilrost::encoding::ForOverwrite for #name where #name: Default {
            fn for_overwrite() -> Self
            where
                Self: Sized,
            {
                #name::default()
            }
        }
    };

    output.into()
}

fn extract_bilorst_as_attr(input: &DeriveInput) -> Result<syn::Type, syn::Error> {
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
