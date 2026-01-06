// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_service_protocol_v4::message_codec::{Decoder, Message, MessageType};
use restate_time_util::DurationExt;
use restate_types::service_protocol::ServiceProtocolVersion;
use std::num::NonZeroUsize;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[error("unexpected lua value received")]
pub struct UnexpectedLuaValue;

macro_rules! set_table_values {
    ($message_table:expr, $($name:expr => $val:expr),* $(,)?) => {
            $($message_table.set($name, $val)?;)*
    };
}

fn decode_packages(lua: &Lua, buf_lua: Value) -> LuaResult<Table> {
    let result_messages = lua.create_table()?;

    // We should store it somewhere, but right now wireshark doesn't support conversations in lua api
    // so we just keep it simple and assume all messages are self contained within the same http data frame
    // https://ask.wireshark.org/question/11650/lua-wireshark-dissector-combine-data-from-2-udp-packets
    let mut dec = Decoder::new(
        ServiceProtocolVersion::V4,
        NonZeroUsize::MAX,
        NonZeroUsize::MAX,
    );

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
            "message" => message.proto_debug()
        );

        // Optional flags
        if let Some(requires_ack) = header.requires_ack() {
            set_table_values!(message_table, "requires_ack" => requires_ack);
        }

        // For some messages, spit out more stuff
        if let Message::Start(start_message) = message {
            set_table_values!(message_table,
                  "start_message_retry_count_since_last_stored_entry" => start_message.retry_count_since_last_stored_entry,
                  "start_message_duration_since_last_stored_entry" => Duration::from_millis(start_message.duration_since_last_stored_entry).friendly().to_string(),
            );
        }

        result_messages.push(message_table)?;
    }

    Ok(result_messages)
}

fn format_message_type(msg_type: MessageType) -> String {
    match msg_type {
        mt @ MessageType::Custom(_) => {
            format!("{mt:?}")
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
