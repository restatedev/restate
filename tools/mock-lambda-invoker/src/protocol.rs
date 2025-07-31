use bytes::{BufMut, Bytes, BytesMut};
use restate_service_protocol_v4::message_codec::{Decoder, Encoder, Message, MessageHeader};
use restate_types::service_protocol::ServiceProtocolVersion;
use std::time::Duration;

pub fn encode_protocol_messages(messages: Vec<Message>) -> anyhow::Result<Bytes> {
    let mut buffer = BytesMut::new();
    let encoder = Encoder::new(ServiceProtocolVersion::V5);

    for message in messages {
        buffer.put(encoder.encode(message));
    }

    Ok(buffer.freeze())
}

pub fn decode_protocol_messages(buffer: Bytes) -> anyhow::Result<Vec<(MessageHeader, Message)>> {
    let mut decoder = Decoder::new(ServiceProtocolVersion::V5, usize::MAX, None);
    decoder.push(buffer);

    let mut protocol_messages = Vec::new();
    while let Some((frame_header, frame)) = decoder.consume_next()? {
        protocol_messages.push((frame_header, frame));
    }

    Ok(protocol_messages)
}

pub fn initial_journal(invocation_id: Bytes, key: Option<Bytes>, input: Bytes) -> Vec<Message> {
    let start_message = Message::new_start_message(
        invocation_id,
        "who cares".to_string(),
        key,
        1,
        false,
        vec![],
        0,
        Duration::from_nanos(0),
    );
    let input_message = Message::InputCommand(input);

    vec![start_message, input_message]
}

pub fn _extend_journal(mut journal: Vec<Message>, new_entries: Vec<Message>) -> Vec<Message> {
    debug_assert!(!journal.is_empty());
    journal.extend(new_entries);
    let know_entries = (journal.len() as u32) - 1;
    match journal.get_mut(0) {
        Some(Message::Start(message)) => {
            message.known_entries = know_entries;
        }
        _ => {
            panic!("unexpected message");
        }
    }
    journal
}
