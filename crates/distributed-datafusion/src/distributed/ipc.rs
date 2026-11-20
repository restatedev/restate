// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One Arrow stream per output lane, carried in complete, single-batch replies.
//!
//! Discard a lane's codec after any error: its dictionary state may have advanced.
//! Neither codec reads ahead, reassembles partial messages, nor supports replay.

use std::io::Cursor;

use bytes::{Bytes, BytesMut};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamEncoder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, exec_datafusion_err, exec_err};

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

    pub(super) fn encode(&mut self, batch: &RecordBatch) -> Result<Bytes> {
        if batch.schema() != self.schema {
            return exec_err!("task batch schema differs from its installed output schema");
        }
        flatten(self.encoder.encode(batch)?)
    }

    pub(super) fn finish(self) -> Result<Bytes> {
        flatten(self.encoder.finish()?)
    }
}

fn flatten(buffers: Vec<Buffer>) -> Result<Bytes> {
    let size = buffers
        .iter()
        .try_fold(0usize, |size, buffer| size.checked_add(buffer.len()))
        .ok_or_else(|| exec_datafusion_err!("task IPC payload size overflow"))?;
    let mut data = BytesMut::with_capacity(size);
    for buffer in buffers {
        data.extend_from_slice(&buffer);
    }
    Ok(data.freeze())
}

pub(super) struct Decoder {
    schema: SchemaRef,
    reader: Option<StreamReader<Cursor<Bytes>>>,
}

impl Decoder {
    pub(super) fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            reader: None,
        }
    }

    pub(super) fn decode(&mut self, data: Bytes) -> Result<RecordBatch> {
        let reader = self.reader(data)?;
        let batch = reader
            .next()
            .transpose()?
            .ok_or_else(|| exec_datafusion_err!("task IPC data reply ended before a batch"))?;
        if batch.schema() != reader.schema() {
            return exec_err!("remote task returned a different output schema");
        }
        if reader.get_ref().position() != reader.get_ref().get_ref().len() as u64 {
            return exec_err!("task IPC data reply contains trailing bytes");
        }
        // Do not call next() again: ordinary EOF would permanently finish the reader.
        // The returned batch owns its Arrow body; release the encoded reply promptly.
        *reader.get_mut() = Cursor::new(Bytes::new());
        Ok(batch)
    }

    pub(super) fn finish(mut self, data: Bytes) -> Result<()> {
        let reader = self.reader(data)?;
        let cursor = reader.get_ref();
        // StreamReader accepts EOF (even a partial prefix) as end of stream. Our
        // fixed nonlegacy V5 profile requires the full eight-byte EOS instead.
        if (cursor.get_ref().len() as u64).checked_sub(cursor.position()) != Some(8) {
            return exec_err!("task IPC terminal reply must contain exactly one EOS");
        }
        if reader.next().transpose()?.is_some()
            || !reader.is_finished()
            || reader.get_ref().position() != reader.get_ref().get_ref().len() as u64
        {
            return exec_err!("invalid task IPC terminal reply");
        }
        Ok(())
    }

    fn reader(&mut self, data: Bytes) -> Result<&mut StreamReader<Cursor<Bytes>>> {
        if let Some(reader) = &mut self.reader {
            if reader.is_finished()
                || reader.get_ref().position() != reader.get_ref().get_ref().len() as u64
            {
                return exec_err!("task IPC reader is not at a live frame boundary");
            }
            *reader.get_mut() = Cursor::new(data);
        } else {
            let reader = StreamReader::try_new(Cursor::new(data), None)?;
            if reader.schema() != self.schema {
                return exec_err!("remote task returned a different output schema");
            }
            self.reader = Some(reader);
        }
        Ok(self.reader.as_mut().expect("reader initialized above"))
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
        let held = left_reader.decode(data.clone()).unwrap();
        assert!(data.is_unique(), "decoder must release the encoded payload");
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
        left_reader.finish(left.finish().unwrap()).unwrap();
        right_reader.finish(right.finish().unwrap()).unwrap();
    }

    #[test]
    fn zero_body_batches_are_data_and_empty_lanes_require_eos() {
        let schema = Arc::new(Schema::empty());
        let mut encoder = Encoder::new(Arc::clone(&schema)).unwrap();
        let mut decoder = Decoder::new(Arc::clone(&schema));
        for rows in [7, 0, 3] {
            let batch = RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap();
            // Each call must return immediately, without the next reply's bytes.
            assert_eq!(
                decoder.decode(encoder.encode(&batch).unwrap()).unwrap(),
                batch
            );
        }
        decoder.finish(encoder.finish().unwrap()).unwrap();

        let terminal = Encoder::new(Arc::clone(&schema)).unwrap().finish().unwrap();
        Decoder::new(Arc::clone(&schema))
            .finish(terminal.clone())
            .unwrap();
        assert!(
            Decoder::new(Arc::clone(&schema))
                .decode(terminal.clone())
                .is_err()
        );
        for len in 0..terminal.len() {
            assert!(
                Decoder::new(Arc::clone(&schema))
                    .finish(terminal.slice(..len))
                    .is_err(),
                "truncated empty-lane terminal at {len}"
            );
        }
    }

    #[test]
    fn rejects_schema_changes_and_incomplete_or_surplus_frames() {
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
        assert!(
            Encoder::new(Arc::clone(&schema))
                .unwrap()
                .encode(&wrong_batch)
                .is_err()
        );
        let mut encoder = Encoder::new(Arc::clone(&schema)).unwrap();
        let first = encoder.encode(&batch).unwrap();
        let second = encoder.encode(&batch).unwrap();
        let terminal = encoder.finish().unwrap();
        assert!(Decoder::new(other).decode(first.clone()).is_err());

        for data in [
            Bytes::new(),
            first.slice(..first.len() - 1),
            [first.as_ref(), second.as_ref()].concat().into(),
            [first.as_ref(), terminal.as_ref()].concat().into(),
        ] {
            assert!(Decoder::new(Arc::clone(&schema)).decode(data).is_err());
        }
        for data in [
            first.clone(), // A batch cannot stand in for a terminal reply.
            Bytes::new(),
            terminal.slice(..4), // A legacy four-byte EOS is not our profile.
            [terminal.as_ref(), &[0]].concat().into(),
            [terminal.as_ref(), terminal.as_ref()].concat().into(),
            Bytes::from_static(&[0; 8]), // EOF after four bytes must not hide leftovers.
        ] {
            let mut decoder = Decoder::new(Arc::clone(&schema));
            assert_eq!(decoder.decode(first.clone()).unwrap(), batch);
            assert!(decoder.finish(data).is_err());
        }
        // A repeated schema on an established lane is invalid, even if it matches.
        let mut decoder = Decoder::new(schema);
        decoder.decode(first.clone()).unwrap();
        assert!(decoder.decode(first).is_err());
    }
}
