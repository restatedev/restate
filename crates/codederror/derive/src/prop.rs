// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ast::{Field, Struct, Variant};

impl Struct<'_> {
    pub(crate) fn code_field(&self) -> Option<&Field<'_>> {
        code_field(&self.fields)
    }
}

impl Variant<'_> {
    pub(crate) fn code_field(&self) -> Option<&Field<'_>> {
        code_field(&self.fields)
    }
}

fn code_field<'a, 'b>(fields: &'a [Field<'b>]) -> Option<&'a Field<'b>> {
    fields
        .iter()
        .find(|&field| field.attrs.code_marker.is_some())
}
