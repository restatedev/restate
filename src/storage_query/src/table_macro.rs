macro_rules! define_builder {
    (DataType::Utf8) => {
        StringBuilder
    };
    (DataType::LargeUtf8) => {
        LargeStringBuilder
    };
    (DataType::Binary) => {
        BinaryBuilder
    };
    (DataType::LargeBinary) => {
        LargeBinaryBuilder
    };
    (DataType::UInt32) => {
        UInt32Builder
    };
    (DataType::UInt64) => {
        UInt64Builder
    };
    (DataType::Int32) => {
        Int32Builder
    };
    (DataType::Date64) => {
        Date64Builder
    };
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
    (DataType::Date64) => {
        i64
    };
    (DataType::UInt64) => {
        u64
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
///    birth_date: DataType::Date64,
/// ))
///
/// ```
///
/// This macro will expend to:
///
///
/// ```ignore
/// struct UserRowBuilderFlags {
///     name: bool,
///     age: bool,
///     secret: bool,
///     birth_date: bool,
/// }
/// pub struct UserRowBuilder<'a> {
///     flags: UserRowBuilderFlags,
///     builder: &'a mut UserBuilder,
/// }
/// impl<'a> UserRowBuilder<'a> {
///     pub fn name(&mut self, value: impl AsRef<str>) {
///         self.builder.name.append_value(value);
///         self.flags.name = true;
///     }
///     pub fn age(&mut self, value: u32) {
///         self.builder.age.append_value(value);
///         self.flags.age = true;
///     }
///     pub fn secret(&mut self, value: impl AsRef<[u8]>) {
///         self.builder.secret.append_value(value);
///         self.flags.secret = true;
///     }
///     pub fn birth_date(&mut self, value: i64) {
///         self.builder.birth_date.append_value(value);
///         self.flags.birth_date = true;
///     }
/// }
/// impl<'a> Drop for UserRowBuilder<'a> {
///     fn drop(&mut self) {
///         if (!self.flags.name) {
///             self.builder.name.append_null();
///         }
///         if (!self.flags.age) {
///             self.builder.age.append_null();
///         }
///         if (!self.flags.secret) {
///             self.builder.secret.append_null();
///         }
///         if (!self.flags.birth_date) {
///             self.builder.birth_date.append_null();
///         }
///     }
/// }
/// pub struct UserBuilder {
///     rows_inserted_so_far: usize,
///     name: StringBuilder,
///     age: UInt32Builder,
///     secret: BinaryBuilder,
///     birth_date: Date64Builder,
/// }
/// impl UserBuilder {
///     pub fn new() -> Self {
///         Self {
///             rows_inserted_so_far: 0,
///             name: <StringBuilder>::new(),
///             age: <UInt32Builder>::new(),
///             secret: <BinaryBuilder>::new(),
///             birth_date: <Date64Builder>::new(),
///         }
///     }
///
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
///                     ::alloc::boxed::Box::new([(Field::new(stringify!( name ), DataType::Utf8, true)), (Field::new(stringify!( age ), DataType::UInt32, true)), (Field::new(stringify!( secret ), DataType::Binary, true)), (Field::new(stringify!( birth_date ), DataType::Date64, true))])
///             )))
///         )
///     }
///
///     pub fn default_capacity() -> usize {
///         1024
///     }
///
///     pub fn full(&self) -> bool {
///         self.rows_inserted_so_far >= Self::default_capacity()
///     }
///
///     pub fn empty(&self) -> bool {
///         self.rows_inserted_so_far == 0
///     }
///
///     pub fn finish(mut self, schema: SchemaRef) -> RecordBatch {
///         let arrays: Vec<ArrayRef> = (<[_]>::into_vec(
///             #[rustc_box]
///                 ::alloc::boxed::Box::new([{
///                 let builder: &mut dyn ArrayBuilder = &mut self.name;
///                 builder.finish()
///             }, {
///                 let builder: &mut dyn ArrayBuilder = &mut self.age;
///                 builder.finish()
///             }, {
///                 let builder: &mut dyn ArrayBuilder = &mut self.secret;
///                 builder.finish()
///             }, {
///                 let builder: &mut dyn ArrayBuilder = &mut self.birth_date;
///                 builder.finish()
///             }])
///         ));
///         RecordBatch::try_new(schema, arrays).unwrap()
///     }
/// }
/// ```
///
/// And it can be used to create RecordBatchs from rows.
///
///
///
macro_rules! define_table {

    ($table_name: ident ($($element: ident : $ty: expr),+ $(,)? ) ) => (paste::paste! {

        pub struct [< $table_name:camel Builder >] {
            rows_inserted_so_far: usize,
            projected_schema: SchemaRef,
            arrays: [< $table_name:camel ArrayBuilder >],
        }

        struct [< $table_name:camel ArrayBuilder >] {
            $($element : Option< define_builder!($ty) > ,)+
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

             fn new(projected_schema: &SchemaRef) -> Self {
                Self {
                    $($element : Self::new_builder(&projected_schema, &stringify!($element)) ,)+
                }
            }

             fn new_builder<T: ArrayBuilder + Default>(projected_schema: &SchemaRef, me: &str) -> Option<T> {
                    if projected_schema.column_with_name(me).is_some() {
                       Some(T::default())
                    } else {
                        None
                    }
             }


             fn finish(mut self) -> Vec<ArrayRef> {
                let arrays = [
                    $(  {
                        self.$element.as_mut().map(|e| {
                            let builder: &mut dyn ArrayBuilder = e;
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

            pub fn new(projected_schema: SchemaRef) -> Self {
                Self {
                    rows_inserted_so_far: 0,
                    arrays:  [< $table_name:camel ArrayBuilder >]::new(&projected_schema),
                    projected_schema,
                }
            }

            #[inline]
            pub fn row(&mut self) -> [< $table_name:camel RowBuilder >] {
                 self.rows_inserted_so_far += 1;

                 [< $table_name:camel RowBuilder >] {
                     builder: self,
                     flags: Default::default(),
                 }
            }

            pub fn schema() -> SchemaRef {
                Arc::new(Schema::new(
                    vec![
                        $(Field::new(stringify!($element), $ty, true),)+
                    ])
                )
            }

            #[inline]
            pub fn default_capacity() -> usize {
                1024
            }

            #[inline]
            pub fn full(&self) -> bool {
                self.rows_inserted_so_far >= Self::default_capacity()
            }

            pub fn empty(&self) -> bool {
                self.rows_inserted_so_far == 0
            }

            pub fn finish(self) -> RecordBatch {
                let arrays = self.arrays.finish();
                RecordBatch::try_new(self.projected_schema, arrays).unwrap()
            }

        }

    })
}

pub(crate) use define_builder;
pub(crate) use define_primitive_trait;
pub(crate) use define_table;
