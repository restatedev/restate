// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, AsArray, BinaryArray, GenericByteArray, PrimitiveArray, RecordBatch,
            StringArray,
        },
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{
            ByteArrayType, DataType, Date64Type, Field, FieldRef, Schema, SchemaRef, TimeUnit,
            TimestampMillisecondType,
        },
        error::ArrowError,
    },
    error::DataFusionError,
    execution::{RecordBatchStream, SendableRecordBatchStream},
};
use futures::{Stream, StreamExt};

pub(super) const V1_CONVERTER: JoinConverter<
    JoinConverter<LargeConverter, FullCountConverter>,
    TimestampConverter,
> = JoinConverter::new(
    JoinConverter::new(LargeConverter, FullCountConverter),
    TimestampConverter,
);

pub(super) struct ConvertRecordBatchStream<C> {
    converter: C,
    inner: SendableRecordBatchStream,
    converted_schema: SchemaRef,
    done: bool,
}

impl<C: Converter> ConvertRecordBatchStream<C> {
    pub(super) fn new(converter: C, inner: SendableRecordBatchStream) -> Self {
        let converted_schema = converter.convert_schema(inner.schema());

        Self {
            converter,
            inner,
            converted_schema,
            done: false,
        }
    }
}

impl<C: Converter> RecordBatchStream for ConvertRecordBatchStream<C> {
    fn schema(&self) -> SchemaRef {
        self.converted_schema.clone()
    }
}

impl<C: Converter> Stream for ConvertRecordBatchStream<C> {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        let record_batch = ready!(self.inner.poll_next_unpin(cx));

        Poll::Ready(match record_batch {
            Some(record_batch) => Some(match record_batch {
                Ok(record_batch) => Ok(self
                    .converter
                    .convert_record_batch(&self.converted_schema, record_batch)?),
                Err(err) => {
                    self.done = true;
                    Err(err)
                }
            }),
            None => None,
        })
    }
}

pub(super) trait Converter: Unpin {
    fn convert_schema(&self, schema: SchemaRef) -> SchemaRef {
        let fields = Vec::from_iter(schema.fields().iter().cloned());
        let fields = self.convert_fields(fields);
        SchemaRef::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
    }

    fn convert_record_batch(
        &self,
        converted_schema: &SchemaRef,
        record_batch: RecordBatch,
    ) -> Result<RecordBatch, ArrowError> {
        let columns = Vec::from_iter(record_batch.columns().iter().cloned());
        let columns = self.convert_columns(converted_schema, columns)?;

        RecordBatch::try_new(converted_schema.clone(), columns)
    }

    fn convert_columns(
        &self,
        converted_schema: &SchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>, ArrowError>;

    fn convert_fields(&self, fields: Vec<FieldRef>) -> Vec<FieldRef>;
}

pub(super) struct JoinConverter<First, Second> {
    first: First,
    second: Second,
}

impl<Before, After> JoinConverter<Before, After> {
    const fn new(before: Before, after: After) -> Self {
        Self {
            first: before,
            second: after,
        }
    }
}

impl<Before: Converter, After: Converter> Converter for JoinConverter<Before, After> {
    fn convert_columns(
        &self,
        converted_schema: &SchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        self.second.convert_columns(
            converted_schema,
            self.first.convert_columns(converted_schema, columns)?,
        )
    }

    fn convert_fields(&self, fields: Vec<FieldRef>) -> Vec<FieldRef> {
        self.second
            .convert_fields(self.first.convert_fields(fields))
    }
}

// Prior to 1.2, we always converted LargeUtf8 to Utf8 and LargeBinary to Binary because
// Arrow JS didn't used to support the Large datatypes.
pub(super) struct LargeConverter;

impl Converter for LargeConverter {
    fn convert_columns(
        &self,
        _converted_schema: &SchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        columns
            .into_iter()
            .map(|column| {
                Ok(match column.data_type() {
                    DataType::LargeBinary => {
                        let col: BinaryArray = convert_array_offset(column.as_binary::<i64>())?;
                        ArrayRef::from(Box::new(col) as Box<dyn Array>)
                    }
                    DataType::LargeUtf8 => {
                        let col: StringArray = convert_array_offset(column.as_string::<i64>())?;
                        ArrayRef::from(Box::new(col) as Box<dyn Array>)
                    }
                    _ => column,
                })
            })
            .collect()
    }

    fn convert_fields(&self, fields: Vec<FieldRef>) -> Vec<FieldRef> {
        fields
            .into_iter()
            .map(|field| {
                let data_type = match field.data_type() {
                    DataType::LargeBinary => DataType::Binary,
                    DataType::LargeUtf8 => DataType::Utf8,
                    other => other.clone(),
                };
                FieldRef::new(Field::new(field.name(), data_type, field.is_nullable()))
            })
            .collect()
    }
}

fn convert_array_offset<Before: ByteArrayType, After: ByteArrayType>(
    array: &GenericByteArray<Before>,
) -> Result<GenericByteArray<After>, ArrowError>
where
    After::Offset: TryFrom<Before::Offset>,
{
    let offsets = array
        .offsets()
        .iter()
        .map(|&o| After::Offset::try_from(o))
        .collect::<Result<ScalarBuffer<After::Offset>, _>>()
        .map_err(|_| ArrowError::CastError("offset conversion failed".into()))?;
    GenericByteArray::<After>::try_new(
        OffsetBuffer::new(offsets),
        array.values().clone(),
        array.nulls().cloned(),
    )
}

// Prior to 1.2, we used a datafusion version which incorrectly considered the results of 'COUNT' statements to be nullable
// This is relevant for a single field name, `full_count` which is used in `inv ls`.
pub(super) struct FullCountConverter;

impl Converter for FullCountConverter {
    fn convert_columns(
        &self,
        _converted_schema: &SchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        // this is a schema-only conversion
        Ok(columns)
    }

    fn convert_fields(&self, fields: Vec<FieldRef>) -> Vec<FieldRef> {
        fields
            .into_iter()
            .map(|field| {
                if field.name().as_str() == "full_count"
                    && field.data_type().eq(&DataType::Int64)
                    && !field.is_nullable()
                {
                    FieldRef::new(Field::clone(&field).with_nullable(true))
                } else {
                    field
                }
            })
            .collect()
    }
}

// Prior to 1.2, we used Date64 fields where we should have used Timestamp fields
// This is relevant for various fields used in the CLI
pub(super) struct TimestampConverter;

impl Converter for TimestampConverter {
    fn convert_columns(
        &self,
        converted_schema: &SchemaRef,
        mut columns: Vec<ArrayRef>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        for (i, field) in converted_schema.fields().iter().enumerate() {
            if let (DataType::Date64, DataType::Timestamp(TimeUnit::Millisecond, _)) =
                (field.data_type(), columns[i].data_type())
            {
                let col = columns[i].as_primitive::<TimestampMillisecondType>();
                // this doesn't copy; the same backing array can be used because they both use i64 epoch-based times
                let col =
                    PrimitiveArray::<Date64Type>::new(col.values().clone(), col.nulls().cloned());
                columns[i] = ArrayRef::from(Box::new(col) as Box<dyn Array>);
            }
        }
        Ok(columns)
    }

    fn convert_fields(&self, fields: Vec<FieldRef>) -> Vec<FieldRef> {
        fields
            .into_iter()
            .map(|field| match (field.name().as_str(), field.data_type()) {
                (
                    // inv ls
                    "last_start_at" | "next_retry_at" | "modified_at" | "created_at" |
                    // inv describe
                    "sleep_wakeup_at",
                    DataType::Timestamp(TimeUnit::Millisecond, _),
                ) => FieldRef::new(Field::clone(&field).with_data_type(DataType::Date64)),
                _ => field,
            })
            .collect()
    }
}
