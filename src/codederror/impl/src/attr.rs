use syn::parse::{Nothing, ParseStream};
use syn::{Attribute, Error as SynError, LitInt, LitStr, Result};

pub struct Attrs<'a> {
    // We parse these just to figure out who should we delegate to during codegen
    pub source: Option<&'a Attribute>,
    pub from: Option<&'a Attribute>,

    // Variant or top level struct/enum attributes
    pub hints: Vec<LitStr>,
    pub code: Option<Code<'a>>,

    // Markers for source and from
    pub hint_marker: Option<&'a Attribute>,
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

    pub fn mark_hint(&mut self, attr: &'a Attribute) -> Result<()> {
        if self.hint_marker.is_some() {
            return Err(SynError::new_spanned(attr, "duplicate #[hint] attribute"));
        }
        self.hint_marker = Some(attr);
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub struct Error<'a> {
    pub original: &'a Attribute,
    pub is_transparent: bool,
}

#[derive(Copy, Clone)]
pub struct Code<'a> {
    pub original: &'a Attribute,
    // If empty -> Unknown
    pub value: Option<u32>,
}

pub fn get(input: &[Attribute]) -> Result<Attrs> {
    let mut attrs = Attrs {
        source: None,
        from: None,

        hints: vec![],
        code: None,

        hint_marker: None,
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
        } else if attr.path.is_ident("hint") {
            parse_hint_attribute(&mut attrs, attr)?;
        } else if attr.path.is_ident("code") {
            parse_code_attribute(&mut attrs, attr)?;
        }
    }

    Ok(attrs)
}

fn parse_hint_attribute<'a>(attrs: &mut Attrs<'a>, attr: &'a Attribute) -> Result<()> {
    if attr.tokens.is_empty() {
        return attrs.mark_hint(attr);
    }

    attr.parse_args_with(|input: ParseStream| {
        attrs.hints.push(input.parse()?);
        Ok(())
    })
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
        } else if let Ok(lit_int) = input.parse::<LitInt>() {
            attrs.code = Some(Code {
                original: attr,
                value: Some(lit_int.base10_parse()?)
            });
            Ok(())
        } else {
            Err(SynError::new_spanned(
                attr,
                "#[code(...)] attribute can contain either an int literal, with the error code, or the unknown keyword",
            ))
        }
    })
}

fn require_empty_attribute(attr: &Attribute) -> Result<()> {
    syn::parse2::<Nothing>(attr.tokens.clone())?;
    Ok(())
}
