// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Maps a hyper connection error to a short, low-cardinality label for the
/// `status` metric. Benign peer disconnects collapse together, while
/// actionable classes (oversized requests, timeouts, protocol parse errors)
/// stay distinguishable.
pub fn hyper_error_status(err: &hyper::Error) -> &'static str {
    if err.is_incomplete_message() {
        // Peer closed before the message completed — common, usually benign.
        "incomplete"
    } else if err.is_timeout() {
        // h1 header-read timeout / h2 keep-alive timeout (slow or stuck peer).
        "timeout"
    } else if err.is_parse_too_large() {
        // Request head (headers/URI) exceeded limits.
        "parse-too-large"
    } else if err.is_parse() {
        // Malformed HTTP, bad status, or h2 preface on an h1 connection.
        "parse"
    } else if err.is_body_write_aborted() {
        // Peer went away while we were writing the response body.
        "body-write-aborted"
    } else if err.is_canceled() {
        "canceled"
    } else if err.is_closed() {
        "closed"
    } else if err.is_shutdown() {
        // Error while shutting down the write half (IO).
        "shutdown"
    } else {
        // Other hyper error — frequently an underlying IO error.
        "io"
    }
}
