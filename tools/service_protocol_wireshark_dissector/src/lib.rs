// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use mlua::prelude::*;
use mlua::{Table, Value};

use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol::message::{Decoder, MessageType, ProtocolMessage};
use restate_types::errors::InvocationError;
use restate_types::journal::raw::RawEntryCodec;

#[derive(Debug, thiserror::Error)]
#[error("unexpected lua value received")]
pub struct UnexpectedLuaValue;

macro_rules! set_table_values {
    ($message_table:expr, $($name:expr => $val:expr),* $(,)?) => {
            $($message_table.set($name, $val)?;)*
    };
}

fn decode_packages<'lua>(lua: &'lua Lua, buf_lua: Value<'lua>) -> LuaResult<Table<'lua>> {
    let result_messages = lua.create_table()?;

    // We should store it somewhere, but right now wireshark doesn't support conversations in lua api
    // so we just keep it simple and assume all messages are self contained within the same http data frame
    // https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwihmdPY68f9AhVPjqQKHQPLBIQQFnoECBEQAQ&url=https%3A%2F%2Fask.wireshark.org%2Fquestion%2F11650%2Flua-wireshark-dissector-combine-data-from-2-udp-packets%2F&usg=AOvVaw0c4wqIkCFxhH57-TRu7wnV
    let mut dec = Decoder::default();

    // Convert the buffer and push it to the decoder
    let buf = match buf_lua {
        Value::String(s) => Bytes::from(s.as_bytes().to_vec()),
        _ => return Err(LuaError::external(UnexpectedLuaValue)),
    };
    dec.push(buf);

    while let Some((header, message)) = dec.consume_next().map_err(LuaError::external)? {
        let message_table = lua.create_table()?;

        // Pass info
        set_table_values!(message_table,
            "ty" => u16::from(header.message_type()),
            "ty_name" => format_message_type(header.message_type()),
            "len" => header.frame_length(),
            "message" => match message {
                ProtocolMessage::Start(m) => {
                    format!("{:#?}", m)
                }
                ProtocolMessage::Completion(c) => {
                    format!("{:#?}", c)
                }
                ProtocolMessage::Suspension(s) => {
                    format!("{:#?}", s)
                }
                ProtocolMessage::Error(e) => {
                    format!("{:?}", InvocationError::from(e))
                }
                ProtocolMessage::UnparsedEntry(e) => {
                    format!("{:#?}", ProtobufRawEntryCodec::deserialize(&e).map_err(LuaError::external)?)
                }
            }
        );

        // Optional flags
        if let Some(protocol_version) = header.protocol_version() {
            set_table_values!(message_table, "protocol_version" => protocol_version);
        }
        if let Some(completed) = header.completed() {
            set_table_values!(message_table, "completed" => completed);
        }
        if let Some(requires_ack) = header.requires_ack() {
            set_table_values!(message_table, "requires_ack" => requires_ack);
        }

        result_messages.push(message_table)?;
    }

    Ok(result_messages)
}

fn format_message_type(msg_type: MessageType) -> String {
    match msg_type {
        mt @ MessageType::Custom(_) => {
            format!("{:?}", mt)
        }
        mt => {
            format!("{:?}({:#06X})", mt, u16::from(mt))
        }
    }
}

#[mlua::lua_module]
fn restate_service_protocol_decoder(lua: &Lua) -> LuaResult<LuaTable> {
    let exports = lua.create_table()?;
    exports.set("decode_packages", lua.create_function(decode_packages)?)?;
    Ok(exports)
}
