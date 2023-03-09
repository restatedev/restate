use proc_macro2::{Ident, TokenStream};
use quote::ToTokens;
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream, Parser};
use syn::{
    braced, token, Attribute, FnArg, Generics, ImplItemConst, ImplItemMethod, ItemImpl, Pat,
    PatIdent, ReturnType, Signature, Token, TraitItemMethod, Type, Visibility,
};

#[derive(Clone)]
pub struct RequestArgument {
    pub name: Ident,
    pub ty: Type,
}

impl Parse for RequestArgument {
    fn parse(input: ParseStream) -> syn::parse::Result<Self> {
        let pat_name: Pat = input.parse()?;

        let name = match pat_name {
            Pat::Ident(PatIdent {
                ident,
                mutability: None,
                subpat: None,
                by_ref: None,
                ..
            }) => ident,
            _ => return Err(input.error(
                "The function arguments can only be in form of 'name: Type', without mut and ref",
            )),
        };
        let _colon: Token![:] = input.parse()?;
        let ty: Type = input.parse()?;

        Ok(Self { name, ty })
    }
}

#[derive(Clone)]
pub struct RequestResponseMethodDefinition {
    pub sig: Signature,
    pub arguments: Vec<RequestArgument>,
    pub return_type: Box<Type>,
}

pub struct RequestResponseChannelInterfaceDefinition {
    pub vis: Visibility,
    pub name: Ident,
    pub methods: Vec<RequestResponseMethodDefinition>,
}

impl Parse for RequestResponseChannelInterfaceDefinition {
    fn parse(input: ParseStream) -> syn::parse::Result<Self> {
        // Parse struct definition
        let vis: Visibility = input.parse()?;
        let _struct_token: token::Struct = input.parse()?;
        let name: Ident = input.parse()?;

        // Parse block containing signatures
        let mut methods = Vec::new();

        let block_content;
        let _brace_token = braced!(block_content in input);

        while !block_content.is_empty() {
            let item: TraitItemMethod = block_content.parse()?;

            // Perform some validation
            if item.default.is_some() {
                return Err(input.error("Method definitions must not have an implementation block"));
            }
            if item.sig.receiver().is_none() {
                return Err(input.error("Method must have &self receiver"));
            }
            if item.sig.asyncness.is_none() {
                return Err(input.error("Method must be async"));
            }
            if item.sig.variadic.is_some() {
                return Err(input.error("Macro doesn't support variadic arguments. Use Vec<_>"));
            }

            let arguments = item
                .sig
                .inputs
                .iter()
                .skip(1)
                .map(|arg|
                    syn::parse2(arg.to_token_stream())
                )
                .collect::<Result<Vec<_>, _>>()?;
            let return_type = match &item.sig.output {
                ReturnType::Type(_, t) => t.clone(),
                _ => return Err(input.error("Method definition must have a response type.")),
            };
            methods.push(RequestResponseMethodDefinition {
                sig: item.sig.clone(),
                arguments,
                return_type,
            })
        }

        Ok(RequestResponseChannelInterfaceDefinition { vis, name, methods })
    }
}
