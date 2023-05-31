use bincode::{DefaultOptions, Options};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::VecDeque;
use std::io::Cursor;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

const ENCODE_ERR_MSG: &str =
    "Failure during encoding a (transient) queue segment to disk. This is an unrecoverable failure at this point.";

const READ_ERR_MSG: &str =
"Failure during reading a (transient) queue segment from disk. This is an unrecoverable failure at this point.";

const DELETE_ERR_MSG: &str =
    "Failure during the removal of a segment queue file. This is an unrecoverable failure at this point.";

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
            let value = DefaultOptions::new()
                .with_varint_encoding()
                .deserialize_from(Cursor::new(&buffer[0..frame_len]))
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

            let value = DefaultOptions::new()
                .with_varint_encoding()
                .deserialize_from(Cursor::new(&frame[0..frame_len]))
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

        DefaultOptions::new()
            .with_varint_encoding()
            .serialize_into(&mut frame, &value)
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
