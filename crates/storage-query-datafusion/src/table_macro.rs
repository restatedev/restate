// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Instead of trying to parse the Timestamp(Millisecond, Some(...)) variant in macros, just use a marker struct
#[allow(dead_code)]
pub struct TimestampMillisecond;

macro_rules! define_builder {
    (DataType::Utf8) => {
        ::datafusion::arrow::array::StringBuilder
    };
    (DataType::LargeUtf8) => {
        ::datafusion::arrow::array::LargeStringBuilder
    };
    (DataType::Binary) => {
        ::datafusion::arrow::array::BinaryBuilder
    };
    (DataType::LargeBinary) => {
        ::datafusion::arrow::array::LargeBinaryBuilder
    };
    (DataType::UInt32) => {
        ::datafusion::arrow::array::UInt32Builder
    };
    (DataType::UInt64) => {
        ::datafusion::arrow::array::UInt64Builder
    };
    (DataType::Int32) => {
        ::datafusion::arrow::array::Int32Builder
    };
    (TimestampMillisecond) => {
        TimestampMillisecondUTCBuilder
    };
    (DataType::Boolean) => {
        ::datafusion::arrow::array::BooleanBuilder
    };
}

// This newtype is necessary to generate values with a UTC timezone, as it will default to having no timezone which can confuse downstream clients
pub struct TimestampMillisecondUTCBuilder(::datafusion::arrow::array::TimestampMillisecondBuilder);

impl Default for TimestampMillisecondUTCBuilder {
    fn default() -> Self {
        Self(
            ::datafusion::arrow::array::TimestampMillisecondBuilder::default()
                .with_timezone(TIMEZONE_UTC.clone()),
        )
    }
}

impl TimestampMillisecondUTCBuilder {
    #[inline]
    pub fn append_value(
        &mut self,
        v: <::datafusion::arrow::datatypes::TimestampMillisecondType as ::datafusion::arrow::array::ArrowPrimitiveType>::Native,
    ) {
        self.0.append_value(v);
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.0.append_null();
    }
}

impl ::datafusion::arrow::array::ArrayBuilder for TimestampMillisecondUTCBuilder {
    fn len(&self) -> usize {
        ::datafusion::arrow::array::ArrayBuilder::len(&self.0)
    }

    fn finish(&mut self) -> datafusion::arrow::array::ArrayRef {
        ::datafusion::arrow::array::ArrayBuilder::finish(&mut self.0)
    }

    fn finish_cloned(&self) -> datafusion::arrow::array::ArrayRef {
        ::datafusion::arrow::array::ArrayBuilder::finish_cloned(&self.0)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        ::datafusion::arrow::array::ArrayBuilder::as_any(&self.0)
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        ::datafusion::arrow::array::ArrayBuilder::as_any_mut(&mut self.0)
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        ::datafusion::arrow::array::ArrayBuilder::into_box_any(Box::new(self.0))
    }
}

macro_rules! define_primitive_trait {
    (DataType::Utf8) => {
        impl AsRef<str>
    };
    (DataType::LargeUtf8) => {
        impl AsRef<str>
    };
    (DataType::Binary) => {
        impl AsRef<[u8]>
    };
    (DataType::LargeBinary) => {
        impl AsRef<[u8]>
    };
    (DataType::UInt32) => {
        u32
    };
    (DataType::Int32) => {
        i32
    };
    (TimestampMillisecond) => {
        i64
    };
    (DataType::UInt64) => {
        u64
    };
    (DataType::Boolean) => {
        bool
    };
}

pub static TIMEZONE_UTC: std::sync::LazyLock<std::sync::Arc<str>> =
    std::sync::LazyLock::new(|| std::sync::Arc::from("+00:00"));

macro_rules! define_data_type {
    (DataType::Utf8) => {
        DataType::Utf8
    };
    (DataType::LargeUtf8) => {
        DataType::LargeUtf8
    };
    (DataType::Binary) => {
        DataType::Binary
    };
    (DataType::LargeBinary) => {
        DataType::LargeBinary
    };
    (DataType::UInt32) => {
        DataType::UInt32
    };
    (DataType::Int32) => {
        DataType::Int32
    };
    (TimestampMillisecond) => {
        DataType::Timestamp(
            ::datafusion::arrow::datatypes::TimeUnit::Millisecond,
            Some(TIMEZONE_UTC.clone()),
        )
    };
    (DataType::UInt64) => {
        DataType::UInt64
    };
    (DataType::Boolean) => {
        DataType::Boolean
    };
}

#[cfg(feature = "table_docs")]
macro_rules! document_type {
    (DataType::Utf8) => {
        "Utf8"
    };
    (DataType::LargeUtf8) => {
        "Utf8"
    };
    (DataType::Binary) => {
        "Binary"
    };
    (DataType::LargeBinary) => {
        "Binary"
    };
    (DataType::UInt32) => {
        "UInt32"
    };
    (DataType::UInt64) => {
        "UInt64"
    };
    (DataType::Int32) => {
        "Int32"
    };
    (TimestampMillisecond) => {
        "TimestampMillisecond"
    };
    (DataType::Boolean) => {
        "Boolean"
    };
}

///
/// Given the following table definition:
///
/// ```ignore
///
///define_table!(user(
///    name: DataType::Utf8,
///    age: DataType::UInt32,
///    secret: DataType::Binary,
///    birth_date: TimestampMillisecond,
/// ))
///
/// ```
///
/// This macro will expand to:
///
///
/// ```ignore
/// pub struct UserBuilder {
///     rows_inserted_so_far: usize,
///     projected_schema: SchemaRef,
///     arrays: UserArrayBuilder,
/// }
/// struct UserArrayBuilder {
///     name: Option<StringBuilder>,
///     age: Option<UInt32Builder>,
///     secret: Option<BinaryBuilder>,
///     birth_date: Option<TimestampMillisecondBuilder>,
/// }
/// pub struct UserRowBuilder<'a> {
///     flags: UserRowBuilderFlags,
///     builder: &'a mut UserBuilder,
/// }
/// #[derive(Default)]
/// struct UserRowBuilderFlags {
///     name: bool,
///     age: bool,
///     secret: bool,
///     birth_date: bool,
/// }
/// impl<'a> UserRowBuilder<'a> {
///     #[inline]
///     pub fn name(&mut self, value: impl AsRef<str>) {
///         if let Some(builder) = self.builder.arrays.name.as_mut() {
///             builder.append_value(value);
///             self.flags.name = true;
///         }
///     }
///
///     #[inline]
///     pub fn is_name_defined(&self) -> bool {
///         self.builder.arrays.name.is_some()
///     }
///     #[inline]
///     pub fn age(&mut self, value: u32) {
///         if let Some(builder) = self.builder.arrays.age.as_mut() {
///             builder.append_value(value);
///             self.flags.age = true;
///         }
///     }
///
///     #[inline]
///     pub fn is_age_defined(&self) -> bool {
///         self.builder.arrays.age.is_some()
///     }
///     #[inline]
///     pub fn secret(&mut self, value: impl AsRef<[u8]>) {
///         if let Some(builder) = self.builder.arrays.secret.as_mut() {
///             builder.append_value(value);
///             self.flags.secret = true;
///         }
///     }
///
///     #[inline]
///     pub fn is_secret_defined(&self) -> bool {
///         self.builder.arrays.secret.is_some()
///     }
///     #[inline]
///     pub fn birth_date(&mut self, value: i64) {
///         if let Some(builder) = self.builder.arrays.birth_date.as_mut() {
///             builder.append_value(value);
///             self.flags.birth_date = true;
///         }
///     }
///
///     #[inline]
///     pub fn is_birth_date_defined(&self) -> bool {
///         self.builder.arrays.birth_date.is_some()
///     }
/// }
/// impl<'a> Drop for UserRowBuilder<'a> {
///     fn drop(&mut self) {
///         if let Some(e) = self.builder.arrays.name.as_mut() {
///             if (!self.flags.name) {
///                 e.append_null();
///             }
///         }
///         if let Some(e) = self.builder.arrays.age.as_mut() {
///             if (!self.flags.age) {
///                 e.append_null();
///             }
///         }
///         if let Some(e) = self.builder.arrays.secret.as_mut() {
///             if (!self.flags.secret) {
///                 e.append_null();
///             }
///         }
///         if let Some(e) = self.builder.arrays.birth_date.as_mut() {
///             if (!self.flags.birth_date) {
///                 e.append_null();
///             }
///         }
///     }
/// }
/// impl UserArrayBuilder {
///     fn new(projected_schema: &SchemaRef) -> Self {
///         Self {
///             name: Self::new_builder(&projected_schema, &stringify!( name )),
///             age: Self::new_builder(&projected_schema, &stringify!( age )),
///             secret: Self::new_builder(&projected_schema, &stringify!( secret )),
///             birth_date: Self::new_builder(&projected_schema, &stringify!( birth_date )),
///         }
///     }
///
///     fn new_builder<T: ArrayBuilder + Default>(projected_schema: &SchemaRef, me: &str) -> Option<T> {
///         if projected_schema.column_with_name(me).is_some() {
///             Some(T::default())
///         } else {
///             None
///         }
///     }
///
///
///     fn finish(mut self) -> Vec<ArrayRef> {
///         let arrays = [
///             {
///                 self.name.as_mut().map(|e| {
///                     let builder: &mut dyn ArrayBuilder = e;
///                     builder.finish()
///                 })
///             }, {
///                 self.age.as_mut().map(|e| {
///                     let builder: &mut dyn ArrayBuilder = e;
///                     builder.finish()
///                 })
///             }, {
///                 self.secret.as_mut().map(|e| {
///                     let builder: &mut dyn ArrayBuilder = e;
///                     builder.finish()
///                 })
///             }, {
///                 self.birth_date.as_mut().map(|e| {
///                     let builder: &mut dyn ArrayBuilder = e;
///                     builder.finish()
///                 })
///             },
///         ];
///
///         arrays.into_iter().flatten().collect()
///     }
/// }
/// impl UserBuilder {
///     pub fn new(projected_schema: SchemaRef) -> Self {
///         Self {
///             rows_inserted_so_far: 0,
///             arrays: UserArrayBuilder::new(&projected_schema),
///             projected_schema,
///         }
///     }
///
///     #[inline]
///     pub fn row(&mut self) -> UserRowBuilder {
///         self.rows_inserted_so_far += 1;
///
///         UserRowBuilder {
///             builder: self,
///             flags: Default::default(),
///         }
///     }
///     pub fn schema() -> SchemaRef {
///         Arc::new(Schema::new(
///             (<[_]>::into_vec(
///                 #[rustc_box]
///                     ::alloc::boxed::Box::new([
///                         (Field::new(stringify!( name ), DataType::Utf8, true)),
///                         (Field::new(stringify!( age ), DataType::UInt32, true)),
///                         (Field::new(stringify!( secret ), DataType::Binary, true)),
///                         (Field::new(stringify!( birth_date ), DataType::Timestamp(TimeUnit::Millisecond, Some(TIMEZONE_UTC.clone())), true))])
///             )))
///         )
///     }
///
///     #[inline]
///     pub fn default_capacity() -> usize {
///         1024
///     }
///
///     #[inline]
///     pub fn full(&self) -> bool {
///         self.rows_inserted_so_far >= Self::default_capacity()
///     }
///
///     pub fn empty(&self) -> bool {
///         self.rows_inserted_so_far == 0
///     }
///
///     pub fn finish(self) -> RecordBatch {
///         let arrays = self.arrays.finish();
///         RecordBatch::try_new(self.projected_schema, arrays).unwrap()
///     }
/// }
/// ```
///
/// And it can be used to create RecordBatches from rows.
macro_rules! define_table {

    ($table_name: ident (
        $(
            $(#[doc = $doc:expr])*
            $element:ident: $ty:expr
        ),+ $(,)?)
    ) => (paste::paste! {

        pub struct [< $table_name:camel Builder >] {
            rows_inserted_so_far: usize,
            projected_schema: ::datafusion::arrow::datatypes::SchemaRef,
            arrays: [< $table_name:camel ArrayBuilder >],
        }

        struct [< $table_name:camel ArrayBuilder >] {
            $(
                $(#[doc = $doc])*
                $element : Option< define_builder!($ty) > ,
            )+
        }

        pub struct [< $table_name:camel RowBuilder >]<'a> {
            flags:  [< $table_name:camel RowBuilderFlags >],
            builder: &'a mut  [< $table_name:camel Builder >],
        }

        #[derive(Default)]
        struct [< $table_name:camel RowBuilderFlags >] {
            $($element : bool, )+
        }
        // --------------------------------------------------------------------------
        // RowBuilder
        // --------------------------------------------------------------------------

        impl<'a> [< $table_name:camel RowBuilder >]<'a> {

                   $(
                        #[inline]
                        pub fn $element(&mut self, value: define_primitive_trait!($ty)) {
                            if let Some(builder) = self.builder.arrays.$element.as_mut() {
                                builder.append_value(value);
                                self.flags.$element = true;
                            }
                       }

                        #[inline]
                        pub fn [<is _ $element _ defined>](&self) -> bool {
                            self.builder.arrays.$element.is_some()
                        }

                    )+
        }

        impl<'a> Drop for [< $table_name:camel RowBuilder >]<'a> {

            fn drop(&mut self) {

                 $(
                        if let Some(e) = self.builder.arrays.$element.as_mut() {
                            if (!self.flags.$element) {
                                e.append_null();
                            }
                        }

                  )+

            }
        }


        // --------------------------------------------------------------------------
        // ArrayBuilder
        // --------------------------------------------------------------------------

        impl [< $table_name:camel ArrayBuilder >] {

             fn new(projected_schema: &::datafusion::arrow::datatypes::SchemaRef) -> Self {
                Self {
                    $($element : Self::new_builder(&projected_schema, &stringify!($element)) ,)+
                }
            }

             fn new_builder<T: ::datafusion::arrow::array::ArrayBuilder + Default>(projected_schema: &::datafusion::arrow::datatypes::SchemaRef, me: &str) -> Option<T> {
                    if projected_schema.column_with_name(me).is_some() {
                       Some(T::default())
                    } else {
                        None
                    }
             }


             fn finish(mut self) -> Vec<::datafusion::arrow::array::ArrayRef> {
                let arrays = [
                    $(  {
                        self.$element.as_mut().map(|e| {
                            let builder: &mut dyn ::datafusion::arrow::array::ArrayBuilder = e;
                            builder.finish()
                        })

                        },
                    )+
                ];

                arrays.into_iter().flatten().collect()
            }
        }

        // --------------------------------------------------------------------------
        // Builder
        // --------------------------------------------------------------------------

        impl [< $table_name:camel Builder >] {
            #[inline]
            pub fn row(&mut self) -> [< $table_name:camel RowBuilder >] {
                 self.rows_inserted_so_far += 1;

                 [< $table_name:camel RowBuilder >] {
                     builder: self,
                     flags: Default::default(),
                 }
            }

            pub fn schema() -> ::datafusion::arrow::datatypes::SchemaRef {
                std::sync::Arc::new(::datafusion::arrow::datatypes::Schema::new(
                    vec![
                        $(::datafusion::arrow::datatypes::Field::new(stringify!($element), define_data_type!($ty), true),)+
                    ])
                )
            }

            #[inline]
            pub fn default_capacity() -> usize {
                1024
            }
        }

        impl $crate::table_util::Builder for [< $table_name:camel Builder >] {
            fn new(projected_schema: ::datafusion::arrow::datatypes::SchemaRef) -> Self {
                Self {
                    rows_inserted_so_far: 0,
                    arrays:  [< $table_name:camel ArrayBuilder >]::new(&projected_schema),
                    projected_schema,
                }
            }

            #[inline]
            fn full(&self) -> bool {
                self.rows_inserted_so_far >= Self::default_capacity()
            }

            fn empty(&self) -> bool {
                self.rows_inserted_so_far == 0
            }

            fn finish(self) -> ::datafusion::common::Result<::datafusion::arrow::record_batch::RecordBatch> {
                let arrays = self.arrays.finish();
                // We add the row count as it wouldn't otherwise work with queries that
                // just run aggregate functions (e.g. COUNT(*)) without selecting fields.
                let options = ::datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(self.rows_inserted_so_far));
                Ok(::datafusion::arrow::record_batch::RecordBatch::try_new_with_options(self.projected_schema, arrays, &options)?)
            }
        }

        // --------------------------------------------------------------------------
        // Docs function
        // --------------------------------------------------------------------------

        #[cfg(feature = "table_docs")]
        pub const TABLE_DOCS: $crate::table_docs::StaticTableDocs = $crate::table_docs::StaticTableDocs {
            name: stringify!($table_name),
            columns: &[
                $(
                    $crate::table_docs::TableColumn {
                        name: stringify!($element),
                        column_type: document_type!($ty),
                        description: concat!($($doc),*)
                    },
                )+
            ],
        };
    })
}

macro_rules! define_sort_order {

    ($table_name: ident (
        $(
            $element:ident
        ),+ $(,)?)
    ) => (paste::paste! {

        pub fn [< $table_name:snake _ sort_order >]() -> Vec<String> {
           vec![ $( stringify!($element).to_string(),)+ ]
        }

        })
}

pub(crate) use define_builder;
pub(crate) use define_data_type;
pub(crate) use define_primitive_trait;
pub(crate) use define_sort_order;
pub(crate) use define_table;
#[cfg(feature = "table_docs")]
pub(crate) use document_type;
