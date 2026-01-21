// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::VecDeque;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

const ENCODE_ERR_MSG: &str = "Failure during encoding a (transient) queue segment to disk. This is an unrecoverable failure at this point.";

const READ_ERR_MSG: &str = "Failure during reading a (transient) queue segment from disk. This is an unrecoverable failure at this point.";

const DELETE_ERR_MSG: &str = "Failure during the removal of a segment queue file. This is an unrecoverable failure at this point.";

pub(crate) async fn consume_segment_infallible<T: DeserializeOwned + Send + 'static>(
    mut base_bath: PathBuf,
    segment_id: u64,
) -> VecDeque<T> {
    base_bath.push(format!("{segment_id}.segment"));
    let f = tokio::fs::File::open(&base_bath).await.expect(READ_ERR_MSG);
    let mut reader = BufReader::with_capacity(1024 * 1024, f);

    //
    // read the total number of elements in this segment.
    //
    let len = reader.read_u32().await.expect(READ_ERR_MSG) as usize;

    //
    // allocate a return buffer
    //
    let mut values = VecDeque::with_capacity(len);

    //
    // read all the values one by one
    //
    let mut frame = Vec::new();
    for _ in 0..len {
        //
        // read a frame
        //
        let frame_len = reader.read_u32().await.expect(READ_ERR_MSG) as usize;
        let buffer = reader.buffer();
        if frame_len <= buffer.len() {
            // avoid copying the data to our internal frame if the underlying BufReader has
            // buffered enough data in its internal buffer.
            let (value, _) = bincode::serde::decode_from_slice(
                &buffer[0..frame_len],
                bincode::config::standard().with_variable_int_encoding(),
            )
            .expect(READ_ERR_MSG);
            reader.consume(frame_len);
            values.push_back(value);
        } else {
            // slow path: copy frame_len worth of bytes from the internal BufReader, and thus
            // force it to fill its internal buffer.

            // make sure that our frame buffer is large enough to hold frame_len bytes
            if frame.len() < frame_len {
                // This will set the len of `frame` to `frame_len` by extending the existing
                // vec with 0u8
                frame.resize(frame_len, 0u8);
            }

            reader
                .read_exact(&mut frame[0..frame_len])
                .await
                .expect(READ_ERR_MSG);

            let (value, _) = bincode::serde::decode_from_slice(
                &frame[0..frame_len],
                bincode::config::standard().with_variable_int_encoding(),
            )
            .expect(READ_ERR_MSG);
            values.push_back(value);
        };
    }

    tokio::fs::remove_file(base_bath)
        .await
        .expect(DELETE_ERR_MSG);

    values
}

pub(crate) async fn create_segment_infallible<T: Serialize + Send + 'static>(
    mut base_path: PathBuf,
    segment_id: u64,
    values: VecDeque<T>,
) {
    base_path.push(format!("{segment_id}.segment"));
    let f = tokio::fs::File::create(&base_path)
        .await
        .expect("Unable to create a file for writing");

    let mut writer = BufWriter::new(f);

    //
    // write the total number of elements in this segment.
    //
    let len = values.len();
    writer.write_u32(len as u32).await.expect(ENCODE_ERR_MSG);

    //
    // write down the elements individually.
    //
    let mut frame = Vec::new();
    for value in values {
        frame.clear();

        bincode::serde::encode_into_std_write(
            value,
            &mut frame,
            bincode::config::standard().with_variable_int_encoding(),
        )
        .expect(ENCODE_ERR_MSG);

        //
        // write down the frame length and the frame body
        //
        writer
            .write_u32(frame.len() as u32)
            .await
            .expect(ENCODE_ERR_MSG);
        writer.write_all(&frame).await.expect(ENCODE_ERR_MSG);
    }
    writer.flush().await.expect(ENCODE_ERR_MSG);
}
