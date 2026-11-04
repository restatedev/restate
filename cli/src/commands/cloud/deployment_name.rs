// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The default deployment name of a connected or tunneled service: `<directory>-<hostname>`,
//! stable across restarts so the service reconnects under the same registered deployment.

pub(super) fn hostname() -> String {
    let host = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_default();
    host.split('.').next().unwrap_or_default().to_owned()
}

/// `<directory>-<hostname>`, sanitised to what the tunnel server accepts.
pub(super) fn default_deployment_name() -> String {
    let dir = std::env::current_dir()
        .ok()
        .and_then(|dir| dir.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "service".to_owned());
    let host = hostname();
    let raw = if host.is_empty() {
        dir
    } else {
        format!("{dir}-{host}")
    };
    sanitize(&raw)
}

pub(super) fn is_valid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}

/// Lowercase, every other character replaced by `-`, no leading, trailing or doubled `-`.
fn sanitize(raw: &str) -> String {
    let mut name = String::with_capacity(raw.len());
    for c in raw.chars().flat_map(char::to_lowercase) {
        if is_valid_char(c) {
            name.push(c);
        } else if !name.ends_with('-') {
            name.push('-');
        }
    }
    let name = name.trim_matches('-');
    if name.is_empty() {
        "service".to_owned()
    } else {
        name.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitises_to_what_the_tunnel_server_accepts() {
        assert_eq!(
            sanitize("My Project-Francesco's MBP"),
            "my-project-francesco-s-mbp"
        );
        assert_eq!(sanitize("--/--"), "service");
    }
}
