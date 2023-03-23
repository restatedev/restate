use crate::ast::{Field, Struct, Variant};

impl Struct<'_> {
    pub(crate) fn code_field(&self) -> Option<&Field> {
        code_field(&self.fields)
    }

    pub(crate) fn hint_field(&self) -> Option<&Field> {
        hint_field(&self.fields)
    }
}

impl Variant<'_> {
    pub(crate) fn code_field(&self) -> Option<&Field> {
        code_field(&self.fields)
    }

    pub(crate) fn hint_field(&self) -> Option<&Field> {
        hint_field(&self.fields)
    }
}

fn code_field<'a, 'b>(fields: &'a [Field<'b>]) -> Option<&'a Field<'b>> {
    for field in fields {
        if field.attrs.code_marker.is_some() {
            return Some(field);
        }
    }
    None
}

fn hint_field<'a, 'b>(fields: &'a [Field<'b>]) -> Option<&'a Field<'b>> {
    for field in fields {
        if field.attrs.hint_marker.is_some() {
            return Some(field);
        }
    }
    None
}
