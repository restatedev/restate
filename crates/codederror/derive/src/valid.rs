// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ast::{Enum, Field, Input, Struct, Variant};
use crate::attr::Attrs;
use syn::{Error, Result};

impl Input<'_> {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Input::Struct(input) => input.validate(),
            Input::Enum(input) => input.validate(),
        }
    }
}

impl Struct<'_> {
    fn validate(&self) -> Result<()> {
        check_non_field_attrs(&self.attrs)?;

        // Check if top level has code or field has code marker
        if !(self.attrs.code.is_some() ^ any_field_has_code_marker(&self.fields)) {
            return Err(Error::new_spanned(
                self.original,
                "cannot find a code to use for the struct. Either annotate the struct with #[code(...)] or annotate a field with #[code], but not both",
            ));
        }

        for field in &self.fields {
            field.validate()?;
        }
        Ok(())
    }
}

impl Enum<'_> {
    fn validate(&self) -> Result<()> {
        check_non_field_attrs(&self.attrs)?;
        for variant in &self.variants {
            variant.validate(self.attrs.code.is_some())?;
        }
        Ok(())
    }
}

impl Variant<'_> {
    #[allow(clippy::nonminimal_bool)]
    fn validate(&self, has_top_code: bool) -> Result<()> {
        check_non_field_attrs(&self.attrs)?;

        let has_code = self.attrs.code.is_some();
        let has_code_marker = any_field_has_code_marker(&self.fields);

        // Check if top level has code or every variant has code or code marker, but not both
        let is_valid = (!has_top_code && !has_code && has_code_marker)
            || (!has_top_code && has_code && !has_code_marker)
            || (has_top_code && !has_code && !has_code_marker)
            || (has_top_code && !has_code && has_code_marker)
            || (has_top_code && has_code && !has_code_marker);

        if !is_valid {
            return Err(Error::new_spanned(
                self.original,
                "cannot find a code for this variant. \
                    Either annotate the enum with #[code(...)] or \
                    annotate the variant with #[code(...)] or \
                    a field inside the variant with #[code]",
            ));
        }
        for field in &self.fields {
            field.validate()?;
        }
        Ok(())
    }
}

fn check_non_field_attrs(attrs: &Attrs) -> Result<()> {
    if let Some(code_marker) = &attrs.code_marker {
        return Err(Error::new_spanned(
            code_marker,
            "not expected here; the #[code] attribute belongs to a field",
        ));
    }
    Ok(())
}

fn any_field_has_code_marker(fields: &[Field]) -> bool {
    fields.iter().any(|field| field.attrs.code_marker.is_some())
}

impl Field<'_> {
    fn validate(&self) -> Result<()> {
        if let Some(code) = &self.attrs.code {
            return Err(Error::new_spanned(
                code.original,
                "not expected here; the #[code(...)] attribute belongs on top of a struct or an enum variant",
            ));
        }
        if self.attrs.code_marker.is_some()
            && self.attrs.from.is_none()
            && self.attrs.source.is_none()
        {
            return Err(Error::new_spanned(
                self.original,
                "#[code] and #[hint] on a field can be used in conjunction only with #[from] or #[source]",
            ));
        }

        Ok(())
    }
}
