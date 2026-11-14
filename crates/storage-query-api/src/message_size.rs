// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Keeping query responses within the message size limit.
//!
//! The node-to-node fabric rejects messages larger than
//! `networking.message-size-limit`, and a rejected message also breaks the
//! connection. Query batches are cut by row count only, so a batch of large rows
//! can encode far beyond that limit: 128 state rows of 1 MiB each make a 128 MiB
//! batch. [`encode_within`] cuts such a batch into row slices that each fit.
//! `RecordBatch::slice` copies nothing, and for the column types V1 tables use,
//! the Arrow IPC writer encodes only a slice's own rows. View types (`Utf8View`,
//! `BinaryView`, `ListView`) and dictionaries are different: a slice still
//! carries its parent's data buffers or whole dictionary, so slicing doesn't make
//! those smaller. Either way, each message's actual encoded size is what's checked.

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;

use restate_types::config::Configuration;
use restate_types::memory::ByteCount;

/// Room left in each message for what surrounds the encoded batch: transport
/// headers, IDs and framing. They take far less than this.
const ENVELOPE_ROOM: usize = 64 * 1024;

/// The most bytes one encoded batch may take in a query response sent to another
/// node, so that the whole message stays within `networking.message-size-limit`.
pub fn response_budget() -> usize {
    let limit = Configuration::pinned()
        .networking
        .message_size_limit
        .as_non_zero_usize()
        .get();
    limit - ENVELOPE_ROOM.min(limit / 2)
}

/// Encodes `batch` as a complete Arrow IPC stream: the schema, the batch and the
/// end marker. A receiver decodes it on its own.
pub fn encode_ipc_stream(schema: &Schema, batch: &RecordBatch) -> Result<Vec<u8>, DataFusionError> {
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, schema)?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(buffer)
}

/// Cuts `batch` into consecutive row slices whose [`encode_ipc_stream`] encoding
/// fits `budget`, and passes each slice with its encoding to `emit`, in row order.
///
/// A batch that fits is passed whole. A larger one is cut into slices sized from
/// how far it overshoots, and each slice is checked again. For example, with a
/// 32 MiB budget, 128 rows of 1 MiB each (about 128 MiB) become four slices of 26
/// rows and one of 24.
///
/// A single row that doesn't fit `budget` with the schema can't be sent: the
/// error names the setting that raises the limit.
pub fn encode_within(
    schema: &Schema,
    batch: RecordBatch,
    budget: usize,
    emit: &mut impl FnMut(RecordBatch, Vec<u8>) -> Result<(), DataFusionError>,
) -> Result<(), DataFusionError> {
    let encoded = encode_ipc_stream(schema, &batch)?;
    if encoded.len() <= budget {
        return emit(batch, encoded);
    }
    let rows = batch.num_rows();
    if rows <= 1 {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "one row of this result, with its schema, encodes to {}, more than one \
             query response can carry ({}); raise `networking.message-size-limit` on \
             all nodes to read it",
            ByteCount::from(encoded.len()),
            ByteCount::from(budget),
        )));
    }
    let rows_per_slice = rows.div_ceil(encoded.len().div_ceil(budget));
    // The slices are encoded anew; don't hold the oversized encoding meanwhile.
    drop(encoded);
    for offset in (0..rows).step_by(rows_per_slice) {
        let slice = batch.slice(offset, rows_per_slice.min(rows - offset));
        encode_within(schema, slice, budget, emit)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        BooleanArray, DictionaryArray, LargeBinaryArray, LargeStringArray, ListBuilder,
        StringBuilder, UInt64Array,
    };
    use datafusion::arrow::buffer::Buffer;
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion::arrow::ipc::reader::StreamDecoder;

    use super::*;

    /// A batch with text, binary, number, boolean, list and dictionary columns,
    /// some with nulls. Each row carries `value_size` bytes of binary data.
    fn batch(rows: u64, value_size: usize) -> RecordBatch {
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let kind = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, true),
            Field::new("value", DataType::LargeBinary, false),
            Field::new("count", DataType::UInt64, false),
            Field::new("done", DataType::Boolean, false),
            Field::new("tags", DataType::List(item), false),
            Field::new("kind", kind, false),
        ]));
        let mut tags = ListBuilder::new(StringBuilder::new());
        for row in 0..rows {
            tags.append_value([Some(format!("tag-{row}")), None]);
        }
        let kinds: DictionaryArray<Int32Type> = (0..rows)
            .map(|row| ["inbox", "running"][usize::from(row % 2 == 1)])
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(LargeStringArray::from_iter(
                    (0..rows).map(|row| (row % 3 != 0).then(|| format!("row-{row}"))),
                )),
                Arc::new(LargeBinaryArray::from_iter_values(
                    (0..rows).map(|row| vec![row as u8; value_size]),
                )),
                Arc::new(UInt64Array::from_iter_values(0..rows)),
                Arc::new(BooleanArray::from_iter(
                    (0..rows).map(|row| Some(row % 2 == 0)),
                )),
                Arc::new(tags.finish()),
                Arc::new(kinds),
            ],
        )
        .unwrap()
    }

    /// Decodes every batch of one Arrow IPC stream.
    fn decode_stream(bytes: &[u8]) -> Vec<RecordBatch> {
        let mut decoder = StreamDecoder::new();
        let mut buffer = Buffer::from(bytes);
        let mut batches = Vec::new();
        while !buffer.is_empty() {
            batches.extend(decoder.decode(&mut buffer).unwrap());
        }
        decoder.finish().unwrap();
        batches
    }

    #[test]
    fn oversized_batches_split_into_slices_that_fit() {
        let batch = batch(100, 1024);
        let schema = batch.schema();
        let within = |batch: &RecordBatch, budget| {
            let mut messages = Vec::new();
            encode_within(&schema, batch.clone(), budget, &mut |_, encoded| {
                messages.push(encoded);
                Ok(())
            })
            .map(|()| messages)
        };
        let budget = 16 * 1024;

        // A batch that fits goes out unchanged, as one message.
        let small = batch.slice(0, 2);
        let expected = encode_ipc_stream(&schema, &small).unwrap();
        assert_eq!(within(&small, budget).unwrap(), [expected]);

        // A larger one is cut into messages that fit, which decode back to the
        // whole batch, in order.
        let messages = within(&batch, budget).unwrap();
        assert!(messages.len() > 1);
        assert!(messages.iter().all(|message| message.len() <= budget));
        let decoded: Vec<_> = messages
            .iter()
            .flat_map(|message| decode_stream(message))
            .collect();
        assert_eq!(concat_batches(&schema, &decoded).unwrap(), batch);

        // Each message carries only its own rows. If slices carried their
        // parent's buffers, the messages would add up to several whole batches.
        let total: usize = messages.iter().map(Vec::len).sum();
        let whole = encode_ipc_stream(&schema, &batch).unwrap().len();
        assert!(total < 2 * whole, "{total} vs {whole}");

        // A single row larger than the budget can't be sent.
        let error = within(&batch, 512).unwrap_err().to_string();
        assert!(error.contains("networking.message-size-limit"), "{error}");
    }
}
