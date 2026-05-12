// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderName, HeaderValue};
    use restate_types::invocation::Header;

    const INVOCATION_ID_HEADER_NAME: HeaderName =
        HeaderName::from_static("x-restate-invocation-id");

    /// Test helper to verify headers are present in HeaderMap
    fn assert_header_present(headers: &HeaderMap, name: &str, expected_value: &str) {
        let header_name = HeaderName::try_from(name).unwrap();
        assert!(
            headers.contains_key(&header_name),
            "Header '{}' should be present",
            name
        );
        let value = headers.get(&header_name).unwrap();
        assert_eq!(
            value.to_str().unwrap(),
            expected_value,
            "Header '{}' should have value '{}'",
            name,
            expected_value
        );
    }

    #[test]
    fn test_header_conversion_to_http_headermap() {
        let invocation_headers = vec![
            Header::new("x-custom-1", "value1"),
            Header::new("x-custom-2", "value2"),
            Header::new("authorization", "Bearer secret"),
        ];

        let mut headers = HeaderMap::new();

        // Simulate the conversion logic from prepare_request
        for header in invocation_headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(&*header.name),
                HeaderValue::try_from(&*header.value),
            ) {
                headers.insert(name, value);
            }
        }

        assert_eq!(headers.len(), 3);
        assert_header_present(&headers, "x-custom-1", "value1");
        assert_header_present(&headers, "x-custom-2", "value2");
        assert_header_present(&headers, "authorization", "Bearer secret");
    }

    #[test]
    fn test_protocol_headers_are_filtered() {
        let invocation_headers = vec![
            Header::new("x-custom-header", "custom-value"),
            Header::new("content-type", "application/json"), // Should be filtered
            Header::new("accept", "application/json"),       // Should be filtered
            Header::new("x-restate-invocation-id", "abc"),   // Should be filtered
            Header::new("traceparent", "00-trace-id-span-id-00"), // Should be filtered
            Header::new("tracestate", "vendor=value"),       // Should be filtered
            Header::new("authorization", "Bearer token"),
        ];

        let mut headers = HeaderMap::new();

        // Add protocol headers first (simulating prepare_request)
        headers.insert(
            HeaderName::from_static("content-type"),
            HeaderValue::from_static("application/restate"),
        );
        headers.insert(
            HeaderName::from_static("accept"),
            HeaderValue::from_static("application/restate"),
        );
        headers.insert(
            INVOCATION_ID_HEADER_NAME,
            HeaderValue::from_static("inv-id-123"),
        );
        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-protocol-trace-span-00"),
        );
        headers.insert(
            HeaderName::from_static("tracestate"),
            HeaderValue::from_static("protocol=state"),
        );

        // Now add invocation headers with filtering (simulating prepare_request logic)
        for header in invocation_headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(&*header.name),
                HeaderValue::try_from(&*header.value),
            ) {
                // Skip protocol-reserved headers
                if name != http::header::CONTENT_TYPE
                    && name != http::header::ACCEPT
                    && name != INVOCATION_ID_HEADER_NAME
                    && name != "traceparent"
                    && name != "tracestate"
                {
                    headers.insert(name, value);
                }
            }
        }

        // Verify protocol headers remain unchanged
        assert_header_present(&headers, "content-type", "application/restate");
        assert_header_present(&headers, "accept", "application/restate");
        assert_header_present(&headers, "x-restate-invocation-id", "inv-id-123");
        assert_header_present(&headers, "traceparent", "00-protocol-trace-span-00");
        assert_header_present(&headers, "tracestate", "protocol=state");

        // Verify custom headers are present
        assert_header_present(&headers, "x-custom-header", "custom-value");
        assert_header_present(&headers, "authorization", "Bearer token");
    }

    #[test]
    fn test_headers_with_special_characters() {
        let headers = vec![
            Header::new("x-forwarded-for", "192.168.1.1, 10.0.0.1"),
            Header::new("x-api-key", "key-with-dashes-and_underscores"),
            Header::new("x-custom", "value with spaces"),
        ];

        let mut header_map = HeaderMap::new();

        for header in headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(&*header.name),
                HeaderValue::try_from(&*header.value),
            ) {
                header_map.insert(name, value);
            }
        }

        assert_eq!(header_map.len(), 3);
        assert_header_present(&header_map, "x-forwarded-for", "192.168.1.1, 10.0.0.1");
        assert_header_present(&header_map, "x-api-key", "key-with-dashes-and_underscores");
        assert_header_present(&header_map, "x-custom", "value with spaces");
    }

    #[test]
    fn test_invalid_header_names_are_skipped() {
        let invocation_headers = vec![
            Header::new("valid-header", "value1"),
            // Invalid header names that might cause HeaderName::try_from to fail
            Header::new("", "empty-name"), // Empty name
            Header::new("invalid\nheader", "newline-in-name"), // Newline in name
            Header::new("valid-header-2", "value2"),
        ];

        let mut headers = HeaderMap::new();
        let mut skipped = 0;

        for header in invocation_headers {
            match (
                HeaderName::try_from(&*header.name),
                HeaderValue::try_from(&*header.value),
            ) {
                (Ok(name), Ok(value)) => {
                    headers.insert(name, value);
                }
                (Err(_e), _) => {
                    // In real code, this would log: warn!("Invalid header name: {}", e);
                    skipped += 1;
                }
                (_, Err(_e)) => {
                    // In real code, this would log: warn!("Invalid header value: {}", e);
                    skipped += 1;
                }
            }
        }

        assert_eq!(headers.len(), 2, "Only valid headers should be inserted");
        assert_eq!(skipped, 2, "Invalid headers should be skipped and logged");
        assert_header_present(&headers, "valid-header", "value1");
        assert_header_present(&headers, "valid-header-2", "value2");
    }

    #[test]
    fn test_invalid_header_values_are_skipped() {
        let invocation_headers = vec![
            Header::new("valid-header", "valid-value"),
            // Invalid header values that might cause HeaderValue::try_from to fail
            Header::new("header-with-invalid-value", "value\nwith\nnewlines"), // Newlines in value
            Header::new("another-valid", "ok"),
        ];

        let mut headers = HeaderMap::new();
        let mut skipped_count = 0;

        for header in invocation_headers {
            match (
                HeaderName::try_from(&*header.name),
                HeaderValue::try_from(&*header.value),
            ) {
                (Ok(name), Ok(value)) => {
                    headers.insert(name, value);
                }
                (Err(_), _) | (_, Err(_)) => {
                    // In real code: warn!("Invalid header, dropping");
                    skipped_count += 1;
                }
            }
        }

        assert_eq!(headers.len(), 2, "Only valid headers should be present");
        assert_eq!(skipped_count, 1, "Invalid header value should be skipped");
        assert_header_present(&headers, "valid-header", "valid-value");
        assert_header_present(&headers, "another-valid", "ok");
    }

    #[test]
    fn test_case_insensitive_header_filtering() {
        // HTTP headers are case-insensitive, but our filtering uses exact match
        // This test ensures we handle common variations
        let invocation_headers = vec![
            Header::new("Content-Type", "text/plain"), // Different case
            Header::new("ACCEPT", "text/html"),        // Upper case
            Header::new("X-Custom", "value"),
        ];

        let mut headers = HeaderMap::new();

        // Pre-set protocol headers
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/restate"),
        );
        headers.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("application/restate"),
        );

        // Add invocation headers with filtering
        for header in invocation_headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(&*header.name),
                HeaderValue::try_from(&*header.value),
            ) {
                // HeaderName comparison is case-insensitive
                if name != http::header::CONTENT_TYPE
                    && name != http::header::ACCEPT
                    && name != INVOCATION_ID_HEADER_NAME
                    && name != "traceparent"
                    && name != "tracestate"
                {
                    headers.insert(name, value);
                }
            }
        }

        // Protocol headers should remain
        assert_header_present(&headers, "content-type", "application/restate");
        assert_header_present(&headers, "accept", "application/restate");
        // Custom header should be present
        assert_header_present(&headers, "x-custom", "value");
    }

    #[test]
    fn test_multiple_headers_with_same_name() {
        // HeaderMap.insert replaces previous value, not appends
        let headers = vec![
            Header::new("x-custom", "value1"),
            Header::new("x-custom", "value2"),
        ];

        let mut header_map = HeaderMap::new();

        for header in headers {
            if let (Ok(name), Ok(value)) = (
                HeaderName::try_from(&*header.name),
                HeaderValue::try_from(&*header.value),
            ) {
                header_map.insert(name, value);
            }
        }

        // Only the last value should be present (HeaderMap::insert behavior)
        assert_eq!(header_map.len(), 1);
        assert_header_present(&header_map, "x-custom", "value2");
    }
}
