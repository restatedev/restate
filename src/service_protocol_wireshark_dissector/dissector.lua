-- Require the decoder library
local decoder = require "restate_service_protocol_decoder"

-- Declare our protocol
local p_service_protocol = Proto("restate_service_protocol", "Restate Service Endpoint Protocol");

-- Define the fields
local f_ty = ProtoField.uint16("restate_service_protocol.message_type", "Message Type", base.HEX)
local f_protocol_version = ProtoField.uint16("restate_service_protocol.protocol_version", "Protocol version")
local f_completed = ProtoField.bool("restate_service_protocol.completed", "Completed flag", base.NONE, {
    "Completed",
    "Not completed"
})
local f_requires_ack = ProtoField.bool("restate_service_protocol.requires_ack", "Requires Ack flag", base.NONE, {
    "Requires ack",
    "Doesn't require ack"
})
local f_len = ProtoField.uint16("restate_service_protocol.length", "Length", base.DEC)
local f_message = ProtoField.string("restate_service_protocol.message", "Message", base.UNICODE)

p_service_protocol.fields = {
    f_ty,
    f_protocol_version,
    f_completed,
    f_requires_ack,
    f_len,
    f_message
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
        subtree:add(f_len, buf(4,8), msg.len)
        if msg.protocol_version ~= nil then
            subtree:add(f_protocol_version, msg.protocol_version)
        end
        if msg.completed ~= nil then
            subtree:add(f_completed, msg.completed)
        end
        if msg.requires_ack ~= nil then
            subtree:add(f_requires_ack, msg.requires_ack)
        end
        subtree:add(f_message, buf(8), msg.message, msg.message)
    end
end

-- Retrieve the media type table (inspired to gRPC dissector code)
-- https://github.com/wireshark/wireshark/blob/master/epan/dissectors/packet-grpc.c
local media_types = DissectorTable.get("media_type")
local streaming_media_types = DissectorTable.get("streaming_content_type")

media_types:add("application/restate", p_service_protocol)
streaming_media_types:add("application/restate", p_service_protocol)
