use std::time::Duration;

use tracing::debug;

/// Parses and extracts the `Retry-After` header from [`http::response::Parts`]
///
/// Returns None if header does not exist or if invalid format
pub(super) fn parse_retry_after(parts: &http::response::Parts) -> Option<Duration> {
    debug!("Parse retry after (Headers): {:?}", parts.headers);

    let value = parts
        .headers
        .get(http::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())?;

    if let Ok(duration) = value.parse::<u64>() {
        return Some(Duration::from_secs(duration));
    }

    if let Ok(deadline) = chrono::DateTime::parse_from_rfc2822(value) {
        let deadline = deadline.to_utc();
        let duration = deadline - chrono::Utc::now();
        let duration = duration.num_seconds();
        if duration.is_positive() {
            return Some(Duration::from_secs(duration as u64));
        } else {
            return None;
        }
    }

    // failed to parse both `Retry-After` date formats
    None
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::parse_retry_after;

    fn parts_with_retry_after(value: &str) -> http::response::Parts {
        let (mut parts, _) = http::Response::new(()).into_parts();
        parts.headers.insert(
            http::header::RETRY_AFTER,
            http::HeaderValue::from_str(value).unwrap(),
        );
        parts
    }

    fn parts_without_retry_after() -> http::response::Parts {
        http::Response::new(()).into_parts().0
    }

    #[test]
    fn test_parse_retry_after_missing_header() {
        assert_eq!(parse_retry_after(&parts_without_retry_after()), None);
    }

    #[test]
    fn test_parse_retry_after_seconds() {
        // valid integer seconds
        assert_eq!(
            parse_retry_after(&parts_with_retry_after("120")),
            Some(Duration::from_secs(120))
        );
        // zero seconds is valid
        assert_eq!(
            parse_retry_after(&parts_with_retry_after("0")),
            Some(Duration::from_secs(0))
        );
    }

    #[test]
    fn test_parse_retry_after_rfc2822_date() {
        // a date far in the future must return Some with a positive duration
        let result = parse_retry_after(&parts_with_retry_after("Fri, 01 Jan 2100 00:00:00 +0000"));
        assert!(result.is_some_and(|d| d > Duration::ZERO));

        // a date in the past returns None (no point in retrying immediately)
        let result = parse_retry_after(&parts_with_retry_after("Tue, 01 Jan 2000 00:00:00 +0000"));
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_retry_after_invalid() {
        assert_eq!(
            parse_retry_after(&parts_with_retry_after("not-a-value")),
            None
        );
        assert_eq!(parse_retry_after(&parts_with_retry_after("-1")), None);
    }
}
