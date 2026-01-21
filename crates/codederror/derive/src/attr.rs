// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Some parts copied from https://github.com/dtolnay/thiserror/blob/39aaeb00ff270a49e3c254d7b38b10e934d3c7a5/impl/src/attr.rs
//! License Apache-2.0 or MIT

use syn::parse::{Nothing, ParseStream};
use syn::{Attribute, Error as SynError, Path, Result};

pub struct Attrs<'a> {
    // We parse these just to figure out who should we delegate to during codegen
    pub source: Option<&'a Attribute>,
    pub from: Option<&'a Attribute>,

    // Variant or top level struct/enum attribute
    pub code: Option<Code<'a>>,

    // Marker for source and from
    pub code_marker: Option<&'a Attribute>,
}

impl<'a> Attrs<'a> {
    pub fn mark_source(&mut self, attr: &'a Attribute) -> Result<()> {
        if self.source.is_some() {
            return Err(SynError::new_spanned(attr, "duplicate #[source] attribute"));
        }
        self.source = Some(attr);
        Ok(())
    }

    pub fn mark_from(&mut self, attr: &'a Attribute) -> Result<()> {
        if self.from.is_some() {
            return Err(SynError::new_spanned(attr, "duplicate #[from] attribute"));
        }
        self.from = Some(attr);
        Ok(())
    }

    pub fn mark_code(&mut self, attr: &'a Attribute) -> Result<()> {
        if self.code_marker.is_some() {
            return Err(SynError::new_spanned(attr, "duplicate #[code] attribute"));
        }
        self.code_marker = Some(attr);
        Ok(())
    }
}

#[derive(Clone)]
pub struct Code<'a> {
    pub original: &'a Attribute,
    // If empty -> Unknown
    pub value: Option<Path>,
}

pub fn get(input: &[Attribute]) -> Result<Attrs<'_>> {
    let mut attrs = Attrs {
        source: None,
        from: None,
        code: None,
        code_marker: None,
    };

    for attr in input {
        if attr.path.is_ident("source") {
            require_empty_attribute(attr)?;
            attrs.mark_source(attr)?;
        } else if attr.path.is_ident("from") {
            if !attr.tokens.is_empty() {
                // Assume this is meant for derive_more crate or something.
                continue;
            }
            attrs.mark_from(attr)?;
        } else if attr.path.is_ident("code") {
            parse_code_attribute(&mut attrs, attr)?;
        }
    }

    Ok(attrs)
}

fn parse_code_attribute<'a>(attrs: &mut Attrs<'a>, attr: &'a Attribute) -> Result<()> {
    if attr.tokens.is_empty() {
        return attrs.mark_code(attr);
    }

    syn::custom_keyword!(unknown);
    attr.parse_args_with(|input: ParseStream| {
        if attrs.code.is_some() {
            return Err(SynError::new_spanned(
                attr,
                "duplicate #[code(...)] attribute",
            ));
        }
        if input.parse::<Option<unknown>>()?.is_some() {
            attrs.code = Some(Code {
                original: attr,
                value: None
            });
            Ok(())
        } else if let Ok(ident) = input.parse::<Path>() {
            attrs.code = Some(Code {
                original: attr,
                value: Some(ident)
            });
            Ok(())
        } else {
            Err(SynError::new_spanned(
                attr,
                "#[code(...)] attribute can contain either an identifier to a const Code instance, or the unknown keyword",
            ))
        }
    })
}

fn require_empty_attribute(attr: &Attribute) -> Result<()> {
    syn::parse2::<Nothing>(attr.tokens.clone())?;
    Ok(())
}
