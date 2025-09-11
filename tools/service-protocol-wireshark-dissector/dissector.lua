-- Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
-- All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Require the decoder library
local decoder = require "restate_service_protocol_decoder"

-- Declare our protocol
local p_service_protocol = Proto("restate_service_protocol", "Restate Service Endpoint Protocol");

-- Define the fields
local f_ty = ProtoField.uint16("restate_service_protocol.message_type", "Message Type", base.HEX)
local f_requires_ack = ProtoField.bool("restate_service_protocol.requires_ack", "REQUIRES_ACK", base.NONE, {
    "Requires ack",
    "Doesn't require ack"
})
local f_len = ProtoField.uint16("restate_service_protocol.length", "Length", base.DEC)
local f_message = ProtoField.string("restate_service_protocol.message", "Message", base.UNICODE)
local f_start_message_retry_count_since_last_stored_entry = ProtoField.uint32("restate_service_protocol.start_message.retry_count_since_last_stored_entry", "Retry count since last stored entry", base.DEC)
local f_start_message_duration_since_last_stored_entry = ProtoField.string("restate_service_protocol.start_message.duration_since_last_stored_entry", "Duration since last stored entry", base.UNICODE)

p_service_protocol.fields = {
    f_ty,
    f_completed,
    f_requires_ack,
    f_len,
    f_message,
    f_start_message_retry_count_since_last_stored_entry,
    f_start_message_duration_since_last_stored_entry
}

-- create a function to dissect it
function p_service_protocol.dissector(buf, pkt, tree)
    --- Invoke rust code to decode frames
    local decode_result = decoder.decode_packages(buf():raw())

    --- One subtree per table result
    for i,msg in ipairs(decode_result) do
        -- Now we can create the subtree
        local subtree = tree:add(p_service_protocol, buf(), "Restate Service Protocol")

        -- Headers (remove them while querying)
        subtree:add(f_ty, buf(0,2), msg.ty, "Message type: " .. msg.ty_name)
        subtree:add(f_len, buf(4,4), msg.len)
        if msg.protocol_version ~= nil then
            subtree:add(f_protocol_version, msg.protocol_version)
        end
        if msg.requires_ack ~= nil then
            subtree:add(f_requires_ack, msg.requires_ack)
        end

        -- Message debug view
        subtree:add(f_message, buf(8), msg.message, msg.message)

        -- Specific messages fields
        if msg.start_message_retry_count_since_last_stored_entry ~= nil then
            subtree:add(f_start_message_retry_count_since_last_stored_entry, msg.start_message_retry_count_since_last_stored_entry)
        end
        if msg.start_message_duration_since_last_stored_entry ~= nil then
            subtree:add(f_start_message_duration_since_last_stored_entry, msg.start_message_duration_since_last_stored_entry)
        end
    end
end

-- Retrieve the media type table (inspired to gRPC dissector code)
-- https://github.com/wireshark/wireshark/blob/master/epan/dissectors/packet-grpc.c
local media_types = DissectorTable.get("media_type")
local streaming_media_types = DissectorTable.get("streaming_content_type")

media_types:add("application/vnd.restate.invocation.v1", p_service_protocol)
streaming_media_types:add("application/vnd.restate.invocation.v1", p_service_protocol)
media_types:add("application/vnd.restate.invocation.v2", p_service_protocol)
streaming_media_types:add("application/vnd.restate.invocation.v2", p_service_protocol)
media_types:add("application/vnd.restate.invocation.v3", p_service_protocol)
streaming_media_types:add("application/vnd.restate.invocation.v3", p_service_protocol)
media_types:add("application/vnd.restate.invocation.v4", p_service_protocol)
streaming_media_types:add("application/vnd.restate.invocation.v4", p_service_protocol)
media_types:add("application/vnd.restate.invocation.v5", p_service_protocol)
streaming_media_types:add("application/vnd.restate.invocation.v5", p_service_protocol)
media_types:add("application/vnd.restate.invocation.v6", p_service_protocol)
streaming_media_types:add("application/vnd.restate.invocation.v6", p_service_protocol)
