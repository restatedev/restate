// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for generated rows and their Arrow representations.

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, GenericByteBuilder, ListBuilder, PrimitiveBuilder,
};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, ByteArrayType, DataType};

use restate_types::vqueues::EntryKind;
use restate_util_string::ReString;

/// Documentation for an exposed column, generated with its representation.
#[derive(Debug, Copy, Clone)]
pub struct TableColumn {
    pub name: &'static str,
    pub column_type: &'static str,
    pub description: &'static str,
}

#[doc(hidden)]
pub trait StringValue {
    fn string_value(&self) -> Option<&str>;
}

impl StringValue for ReString {
    fn string_value(&self) -> Option<&str> {
        Some(self.as_str())
    }
}

impl StringValue for String {
    fn string_value(&self) -> Option<&str> {
        Some(self.as_str())
    }
}

impl StringValue for EntryKind {
    fn string_value(&self) -> Option<&str> {
        Some(self.as_str())
    }
}

impl<V: StringValue> StringValue for Option<V> {
    fn string_value(&self) -> Option<&str> {
        self.as_ref().and_then(StringValue::string_value)
    }
}

/// Borrows binary data or list elements, preserving NULL versus empty values.
#[doc(hidden)]
pub trait SliceValue {
    type Item;

    fn slice_value(&self) -> Option<&[Self::Item]>;
}

impl<V> SliceValue for Vec<V> {
    type Item = V;

    fn slice_value(&self) -> Option<&[V]> {
        Some(self)
    }
}

impl<V: SliceValue> SliceValue for Option<V> {
    type Item = V::Item;

    fn slice_value(&self) -> Option<&[Self::Item]> {
        self.as_ref().and_then(SliceValue::slice_value)
    }
}

#[doc(hidden)]
pub trait PrimitiveValue<V> {
    fn primitive_value(&self) -> Option<V>;
}

impl<V: Copy> PrimitiveValue<V> for V {
    fn primitive_value(&self) -> Option<V> {
        Some(*self)
    }
}

impl<V: Copy> PrimitiveValue<V> for Option<V> {
    fn primitive_value(&self) -> Option<V> {
        *self
    }
}

/// Size the next batch from the previous one, including variable-width children.
/// Rebuilding must preserve Arrow metadata, notably the timestamp timezone.
#[doc(hidden)]
pub trait BuilderCapacity: ArrayBuilder {
    type Capacity;

    fn capacity_for_next_batch(&self) -> Self::Capacity;
    fn with_capacity(capacity: Self::Capacity, data_type: &DataType) -> Self;

    fn finish_and_new(&mut self) -> ArrayRef
    where
        Self: Sized,
    {
        let capacity = self.capacity_for_next_batch();
        let array = self.finish();
        *self = Self::with_capacity(capacity, array.data_type());
        array
    }
}

impl<T: ByteArrayType> BuilderCapacity for GenericByteBuilder<T> {
    type Capacity = (usize, usize);

    fn capacity_for_next_batch(&self) -> Self::Capacity {
        (self.len(), self.values_slice().len())
    }

    fn with_capacity((rows, bytes): Self::Capacity, _: &DataType) -> Self {
        Self::with_capacity(rows, bytes)
    }
}

impl<T: ArrowPrimitiveType> BuilderCapacity for PrimitiveBuilder<T> {
    type Capacity = usize;

    fn capacity_for_next_batch(&self) -> Self::Capacity {
        self.len()
    }

    fn with_capacity(rows: usize, data_type: &DataType) -> Self {
        Self::with_capacity(rows).with_data_type(data_type.clone())
    }
}

impl BuilderCapacity for BooleanBuilder {
    type Capacity = usize;

    fn capacity_for_next_batch(&self) -> Self::Capacity {
        self.len()
    }

    fn with_capacity(rows: usize, _: &DataType) -> Self {
        Self::with_capacity(rows)
    }
}

impl<T: BuilderCapacity> BuilderCapacity for ListBuilder<T> {
    type Capacity = (usize, T::Capacity);

    fn capacity_for_next_batch(&self) -> Self::Capacity {
        (self.len(), self.values_ref().capacity_for_next_batch())
    }

    fn with_capacity((rows, items): Self::Capacity, data_type: &DataType) -> Self {
        let DataType::List(field) = data_type else {
            unreachable!("ListBuilder produces a List array")
        };
        Self::with_capacity(T::with_capacity(items, field.data_type()), rows)
            .with_field(field.clone())
    }
}
