// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate service protocol, which is
// released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/proto/blob/main/LICENSE

tonic::include_proto!("restate.log_server");

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("log_server_svc_descriptor");
