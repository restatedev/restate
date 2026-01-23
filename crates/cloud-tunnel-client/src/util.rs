// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use bytes::{BufMut, BytesMut};
use http::Uri;

pub fn parse_tunnel_destination(
    path_and_query: &http::uri::PathAndQuery,
) -> Result<Uri, &'static str> {
    let path = path_and_query
        .path()
        .strip_prefix('/')
        .ok_or("no leading /")?;
    let (scheme, path) = path.split_once("/").ok_or("no host")?;
    let scheme = http::uri::Scheme::from_str(scheme)
        .ok()
        .ok_or("invalid scheme")?;
    let (host, path) = path.split_once("/").ok_or("no port")?;

    let (port, path) = match path.split_once("/") {
        Some((port, path)) => (port, path),
        None => (path, ""),
    };

    let mut authority = BytesMut::with_capacity(host.len() + 1 + port.len());
    authority.put(host.as_bytes());
    authority.put(&b":"[..]);
    authority.put(port.as_bytes());

    let authority = http::uri::Authority::from_maybe_shared(authority.freeze())
        .ok()
        .ok_or("invalid authority")?;

    let path_and_query = if let Some(query) = path_and_query.query() {
        let mut path_and_query = BytesMut::with_capacity(1 + path.len() + 1 + query.len());
        path_and_query.put(&b"/"[..]);
        path_and_query.put(path.as_bytes());
        path_and_query.put(&b"?"[..]);
        path_and_query.put(query.as_bytes());

        http::uri::PathAndQuery::from_maybe_shared(path_and_query.freeze())
    } else {
        let mut path_and_query = BytesMut::with_capacity(1 + path.len());
        path_and_query.put(&b"/"[..]);
        path_and_query.put(path.as_bytes());

        http::uri::PathAndQuery::from_maybe_shared(path_and_query.freeze())
    };
    let path_and_query = path_and_query.ok().ok_or("invalid path")?;

    Uri::builder()
        .scheme(scheme)
        .authority(authority)
        .path_and_query(path_and_query)
        .build()
        .ok()
        .ok_or("invalid uri")
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::uri::PathAndQuery;

    #[test]
    fn test_parse_tunnel_destination_valid_http() {
        let path_and_query =
            PathAndQuery::from_static("/http/example.com/8080/api/v1/users?id=123");
        let result = parse_tunnel_destination(&path_and_query).unwrap();

        assert_eq!(result.scheme_str(), Some("http"));
        assert_eq!(result.authority().unwrap().as_str(), "example.com:8080");
        assert_eq!(
            result.path_and_query().unwrap().as_str(),
            "/api/v1/users?id=123"
        );
    }

    #[test]
    fn test_parse_tunnel_destination_valid_https() {
        let path_and_query = PathAndQuery::from_static("/https/api.example.com/443/v2/data");
        let result = parse_tunnel_destination(&path_and_query).unwrap();

        assert_eq!(result.scheme_str(), Some("https"));
        assert_eq!(result.authority().unwrap().as_str(), "api.example.com:443");
        assert_eq!(result.path_and_query().unwrap().as_str(), "/v2/data");
    }

    #[test]
    fn test_parse_tunnel_destination_root_path() {
        let path_and_query = PathAndQuery::from_static("/http/localhost/3000");
        let result = parse_tunnel_destination(&path_and_query).unwrap();

        assert_eq!(result.scheme_str(), Some("http"));
        assert_eq!(result.authority().unwrap().as_str(), "localhost:3000");
        assert_eq!(result.path_and_query().unwrap().as_str(), "/");
    }

    #[test]
    fn test_parse_tunnel_destination_with_query_no_path() {
        let path_and_query = PathAndQuery::from_static("/https/example.com/443?query=value");
        let result = parse_tunnel_destination(&path_and_query).unwrap();

        assert_eq!(result.scheme_str(), Some("https"));
        assert_eq!(result.authority().unwrap().as_str(), "example.com:443");
        assert_eq!(result.path_and_query().unwrap().as_str(), "/?query=value");
    }

    #[test]
    fn test_parse_tunnel_destination_missing_leading_slash() {
        let path_and_query = PathAndQuery::from_static("http/example.com/8080/api");
        let result = parse_tunnel_destination(&path_and_query);

        assert_eq!(result, Err("no leading /"));
    }

    #[test]
    fn test_parse_tunnel_destination_no_host() {
        let path_and_query = PathAndQuery::from_static("/http");
        let result = parse_tunnel_destination(&path_and_query);

        assert_eq!(result, Err("no host"));
    }

    #[test]
    fn test_parse_tunnel_destination_invalid_scheme() {
        let path_and_query = PathAndQuery::from_static("/!/example.com/8080/api");
        let result = parse_tunnel_destination(&path_and_query);

        assert_eq!(result, Err("invalid scheme"));
    }

    #[test]
    fn test_parse_tunnel_destination_no_port() {
        let path_and_query = PathAndQuery::from_static("/http/example.com");
        let result = parse_tunnel_destination(&path_and_query);

        assert_eq!(result, Err("no port"));
    }

    #[test]
    fn test_parse_tunnel_destination_complex_path() {
        let path_and_query = PathAndQuery::from_static(
            "/https/api.service.com/443/v1/users/123/posts?limit=10&offset=20",
        );
        let result = parse_tunnel_destination(&path_and_query).unwrap();

        assert_eq!(result.scheme_str(), Some("https"));
        assert_eq!(result.authority().unwrap().as_str(), "api.service.com:443");
        assert_eq!(
            result.path_and_query().unwrap().as_str(),
            "/v1/users/123/posts?limit=10&offset=20"
        );
    }
}
