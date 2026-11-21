// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One Arrow stream per output lane, carried one batch per reply.
//!
//! A lane's first reply starts with the stream's schema; every reply carries
//! the dictionaries its batch needs and exactly one batch. The lane's end is
//! its own reply, not an Arrow end-of-stream marker.
//!
//! Discard a lane's codec after any error: its dictionary state may have
//! advanced. Neither codec supports replay.

use bytes::Bytes;
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamDecoder;
use datafusion::arrow::ipc::writer::StreamEncoder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, exec_err};

use restate_types::net::distributed_query::QueryTaskBatch;

pub(super) struct Encoder {
    schema: SchemaRef,
    encoder: StreamEncoder,
}

impl Encoder {
    pub(super) fn new(schema: SchemaRef) -> Result<Self> {
        Ok(Self {
            encoder: StreamEncoder::try_new(&schema)?,
            schema,
        })
    }

    /// The buffers of the encoded batch, as they are: the reply's encoding
    /// writes them into the message.
    pub(super) fn encode(&mut self, batch: &RecordBatch) -> Result<QueryTaskBatch> {
        if batch.schema_ref() != &self.schema {
            return exec_err!("task batch schema differs from its installed output schema");
        }
        Ok(self.encoder.encode(batch)?.into())
    }
}

pub(super) struct Decoder {
    schema: SchemaRef,
    decoder: StreamDecoder,
}

impl Decoder {
    pub(super) fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            decoder: StreamDecoder::new(),
        }
    }

    /// Decodes a reply. The batch's buffers borrow `data` where it is aligned
    /// for them, and keep it alive.
    pub(super) fn decode(&mut self, data: QueryTaskBatch) -> Result<RecordBatch> {
        let first = self.decoder.schema().is_none();
        let mut data = data.into_buffer();
        let batch = match self.decoder.decode(&mut data)? {
            Some(batch) if data.is_empty() => batch,
            Some(_) => return exec_err!("task output reply continues after its batch"),
            None => {
                // StreamDecoder yields a batch only while it has input left,
                // so a batch without a body, such as one without columns, stays
                // pending at the end of its reply. A byte it leaves unread lets
                // it through; if it reads the byte, the reply ended too early.
                let mut rest = Buffer::from(Bytes::from_static(&[0]));
                match self.decoder.decode(&mut rest)? {
                    Some(batch) if !rest.is_empty() => batch,
                    _ => return exec_err!("task output reply ended before a batch"),
                }
            }
        };
        if first && batch.schema_ref() != &self.schema {
            return exec_err!("remote task returned a different output schema");
        }
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayRef, DictionaryArray, Int32Array, RecordBatchOptions, StringArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};

    use super::*;

    fn dictionary_batch(schema: &SchemaRef, values: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0, 1, 0]), values)
                    .unwrap(),
            )],
        )
        .unwrap()
    }

    #[test]
    fn dictionaries_are_persistent_and_lane_local() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let values = Arc::new(StringArray::from(vec!["first", "second"])) as ArrayRef;
        let first = dictionary_batch(&schema, Arc::clone(&values));
        let repeated = dictionary_batch(&schema, values);
        let replaced = dictionary_batch(
            &schema,
            Arc::new(StringArray::from(vec!["replacement", "other"])),
        );
        let mut left = Encoder::new(Arc::clone(&schema)).unwrap();
        let mut right = Encoder::new(Arc::clone(&schema)).unwrap();
        let mut left_reader = Decoder::new(Arc::clone(&schema));
        let mut right_reader = Decoder::new(schema);
        let data = left.encode(&first).unwrap();
        let initial_len = data.len();
        let held = left_reader.decode(data).unwrap();
        assert_eq!(held, first);
        assert_eq!(
            right_reader
                .decode(right.encode(&replaced).unwrap())
                .unwrap(),
            replaced
        );
        let repeat = left.encode(&repeated).unwrap();
        let repeat_len = repeat.len();
        assert!(repeat_len < initial_len, "schema/dictionary are not resent");
        assert_eq!(left_reader.decode(repeat).unwrap(), repeated);
        let replacement = left.encode(&replaced).unwrap();
        assert!(
            repeat_len < replacement.len(),
            "changed dictionary must be sent"
        );
        assert_eq!(left_reader.decode(replacement).unwrap(), replaced);
        assert_eq!(
            held, first,
            "dictionary replacement must not mutate old rows"
        );
        assert_eq!(
            right_reader.decode(right.encode(&first).unwrap()).unwrap(),
            first
        );
    }

    #[test]
    fn a_reply_is_exactly_one_batch_of_the_installed_schema() {
        // A batch without columns, or without rows, is still a batch.
        let empty = Arc::new(Schema::empty());
        let mut encoder = Encoder::new(Arc::clone(&empty)).unwrap();
        let mut decoder = Decoder::new(Arc::clone(&empty));
        for rows in [7, 0] {
            let batch = RecordBatch::try_new_with_options(
                Arc::clone(&empty),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap();
            assert_eq!(
                decoder.decode(encoder.encode(&batch).unwrap()).unwrap(),
                batch
            );
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let other = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )]));
        let wrong_batch = RecordBatch::try_new(
            Arc::clone(&other),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let mut encoder = Encoder::new(Arc::clone(&schema)).unwrap();
        assert!(encoder.encode(&wrong_batch).is_err());
        // As received: one buffer of the reply's bytes.
        let first = Bytes::copy_from_slice(&encoder.encode(&batch).unwrap().into_buffer());
        let second = Bytes::copy_from_slice(&encoder.encode(&batch).unwrap().into_buffer());
        assert!(Decoder::new(other).decode(first.clone().into()).is_err());
        for data in [
            Bytes::new(),
            first.slice(..first.len() - 1),
            [first.as_ref(), second.as_ref()].concat().into(),
        ] {
            assert!(
                Decoder::new(Arc::clone(&schema))
                    .decode(data.into())
                    .is_err()
            );
        }
        // A lane's schema comes once: a second stream can't continue the first.
        let mut decoder = Decoder::new(schema);
        assert_eq!(decoder.decode(first.clone().into()).unwrap(), batch);
        assert!(decoder.decode(first.into()).is_err());
    }
}
