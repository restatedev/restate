// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    fmt::{self, Display},
    num::{NonZeroU32, ParseIntError},
    str::FromStr,
};

use serde::{Deserialize, Serialize, de::Visitor};

/// A rate specification that can be expressed in different time units.
///
/// This type represents a rate (e.g., requests per second, tokens per minute) with support
/// for multiple time units. It provides flexible parsing from various string formats and
/// comprehensive serialization support.
///
/// # Examples
///
/// ```rust
/// use restate_types::rate::Rate;
/// use std::num::NonZeroU32;
///
/// // Create rates using different time units
/// let per_second = Rate::PerSecond(NonZeroU32::new(100).unwrap());
/// let per_minute = Rate::PerMinute(NonZeroU32::new(60).unwrap());
/// let per_hour = Rate::PerHour(NonZeroU32::new(3600).unwrap());
///
/// // Parse from string
/// let parsed: Rate = "100/sec".parse().unwrap();
/// assert_eq!(parsed, per_second);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rate {
    /// Rate expressed as operations per second
    PerSecond(NonZeroU32),
    /// Rate expressed as operations per minute
    PerMinute(NonZeroU32),
    /// Rate expressed as operations per hour
    PerHour(NonZeroU32),
}

impl Rate {
    /// Returns the numeric value of the rate, regardless of the time unit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use restate_types::rate::Rate;
    /// use std::num::NonZeroU32;
    ///
    /// let rate = Rate::PerMinute(NonZeroU32::new(60).unwrap());
    /// assert_eq!(rate.get(), NonZeroU32::new(60).unwrap());
    /// ```
    pub fn get(&self) -> NonZeroU32 {
        match self {
            Rate::PerSecond(rate) => *rate,
            Rate::PerMinute(rate) => *rate,
            Rate::PerHour(rate) => *rate,
        }
    }
}

impl Display for Rate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Rate::PerSecond(rate) => write!(f, "{}/s", rate),
            Rate::PerMinute(rate) => write!(f, "{}/m", rate),
            Rate::PerHour(rate) => write!(f, "{}/h", rate),
        }
    }
}

/// Errors that can occur when parsing a rate from a string.
///
/// This enum provides detailed error information for different types of parsing failures,
/// making it easier to provide meaningful error messages to users.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// The input string does not match the expected rate format.
    ///
    /// Expected format: `<rate>/<unit>` where `<unit>` is one of:
    /// - `s`, `sec`, or `second` for seconds
    /// - `m`, `min`, or `minute` for minutes  
    /// - `h`, `hr`, or `hour` for hours
    #[error("invalid rate syntax expected format: <rate>/<unit>")]
    InvalidSyntax,
    /// The rate value could not be parsed as a valid non-zero integer.
    ///
    /// This includes cases where the rate is zero, negative, or contains non-numeric characters.
    #[error("invalid rate: {0}")]
    InvalidRate(ParseIntError),
}

impl FromStr for Rate {
    type Err = ParseError;

    /// Parses a rate from a string representation.
    ///
    /// This method supports multiple input formats:
    /// - Plain numbers (e.g., `"100"`) - defaults to per-second
    /// - Rate with unit (e.g., `"100/sec"`, `"60/min"`, `"3600/hr"`)
    /// - Case-insensitive units (`"100/SEC"`, `"60/MIN"`, `"3600/HR"`)
    /// - Full unit names (`"100/second"`, `"60/minute"`, `"3600/hour"`)
    /// - Whitespace-tolerant (`" 100 / sec "`)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use restate_types::rate::Rate;
    /// use std::num::NonZeroU32;
    ///
    /// // Plain number defaults to per-second
    /// let rate: Rate = "100".parse().unwrap();
    /// assert_eq!(rate, Rate::PerSecond(NonZeroU32::new(100).unwrap()));
    ///
    /// // Explicit unit specification
    /// let rate: Rate = "60/min".parse().unwrap();
    /// assert_eq!(rate, Rate::PerMinute(NonZeroU32::new(60).unwrap()));
    ///
    /// // Case-insensitive and whitespace-tolerant
    /// let rate: Rate = " 3600 / HOUR ".parse().unwrap();
    /// assert_eq!(rate, Rate::PerHour(NonZeroU32::new(3600).unwrap()));
    /// ```
    ///
    /// # Errors
    ///
    /// Returns `ParseError::InvalidSyntax` for malformed input or unsupported units.
    /// Returns `ParseError::InvalidRate` for invalid numeric values (zero, negative, non-numeric).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (rate, unit) = match s.split_once('/') {
            Some((rate, unit)) => (rate, unit),
            None => {
                let rate = s
                    .trim()
                    .parse::<NonZeroU32>()
                    .map_err(ParseError::InvalidRate)?;
                return Ok(Rate::PerSecond(rate));
            }
        };

        let rate = rate
            .trim()
            .parse::<NonZeroU32>()
            .map_err(ParseError::InvalidRate)?;

        match unit.trim().to_lowercase().as_str() {
            "s" | "sec" | "second" => Ok(Rate::PerSecond(rate)),
            "m" | "min" | "minute" => Ok(Rate::PerMinute(rate)),
            "h" | "hr" | "hour" => Ok(Rate::PerHour(rate)),
            _ => Err(ParseError::InvalidSyntax),
        }
    }
}

impl Serialize for Rate {
    /// Serializes the rate as a string in the format `<rate>/<unit>`.
    ///
    /// The serialized format uses abbreviated units: `sec`, `min`, `hr`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use restate_types::rate::Rate;
    /// use std::num::NonZeroU32;
    /// use serde_json;
    ///
    /// let rate = Rate::PerMinute(NonZeroU32::new(60).unwrap());
    /// let serialized = serde_json::to_string(&rate).unwrap();
    /// assert_eq!(serialized, "\"60/m\"");
    /// ```
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct RateVisitor;

impl<'de> Visitor<'de> for RateVisitor {
    type Value = Rate;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("rate number or string in the format <rate>/<unit>")
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(Rate::PerSecond(
            NonZeroU32::new(u32::try_from(value).map_err(E::custom)?)
                .ok_or_else(|| E::custom("rate must be non-zero"))?,
        ))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Rate::from_str(value).map_err(serde::de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Rate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(RateVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    #[test]
    fn test_serialize_per_second() {
        let rate = Rate::PerSecond(NonZeroU32::new(100).unwrap());
        let serialized = serde_json::to_string(&rate).unwrap();
        assert_eq!(serialized, "\"100/s\"");
    }

    #[test]
    fn test_serialize_per_minute() {
        let rate = Rate::PerMinute(NonZeroU32::new(60).unwrap());
        let serialized = serde_json::to_string(&rate).unwrap();
        assert_eq!(serialized, "\"60/m\"");
    }

    #[test]
    fn test_serialize_per_hour() {
        let rate = Rate::PerHour(NonZeroU32::new(3600).unwrap());
        let serialized = serde_json::to_string(&rate).unwrap();
        assert_eq!(serialized, "\"3600/h\"");
    }

    #[test]
    fn test_deserialize_per_second() {
        let json = "\"100/s\"";
        let rate: Rate = serde_json::from_str(json).unwrap();
        assert_eq!(rate, Rate::PerSecond(NonZeroU32::new(100).unwrap()));
    }

    #[test]
    fn test_deserialize_per_minute() {
        let json = "\"60/m\"";
        let rate: Rate = serde_json::from_str(json).unwrap();
        assert_eq!(rate, Rate::PerMinute(NonZeroU32::new(60).unwrap()));
    }

    #[test]
    fn test_deserialize_per_hour() {
        let json = "\"3600/h\"";
        let rate: Rate = serde_json::from_str(json).unwrap();
        assert_eq!(rate, Rate::PerHour(NonZeroU32::new(3600).unwrap()));
    }

    #[test]
    fn test_deserialize_default_per_second() {
        let json = "\"100\"";
        let rate: Rate = serde_json::from_str(json).unwrap();
        assert_eq!(rate, Rate::PerSecond(NonZeroU32::new(100).unwrap()));
    }

    #[test]
    fn test_deserialize_case_insensitive() {
        let test_cases = vec![
            ("\"100/S\"", Rate::PerSecond(NonZeroU32::new(100).unwrap())),
            ("\"100/M\"", Rate::PerMinute(NonZeroU32::new(100).unwrap())),
            ("\"100/H\"", Rate::PerHour(NonZeroU32::new(100).unwrap())),
        ];

        for (json, expected) in test_cases {
            let rate: Rate = serde_json::from_str(json).unwrap();
            assert_eq!(rate, expected);
        }
    }

    #[test]
    fn test_deserialize_with_whitespace() {
        let test_cases = vec![
            (
                "\" 100 / second \"",
                Rate::PerSecond(NonZeroU32::new(100).unwrap()),
            ),
            (
                "\" 60 / minute \"",
                Rate::PerMinute(NonZeroU32::new(60).unwrap()),
            ),
            (
                "\" 3600 / hour \"",
                Rate::PerHour(NonZeroU32::new(3600).unwrap()),
            ),
        ];

        for (json, expected) in test_cases {
            let rate: Rate = serde_json::from_str(json).unwrap();
            assert_eq!(rate, expected);
        }
    }

    #[test]
    fn test_deserialize_invalid_syntax() {
        let invalid_cases = vec![
            "\"100/x\"", // invalid unit
            "\"100/\"",  // missing unit
            "\"/s\"",    // missing rate
            "\"100s\"",  // missing slash
            "\"\"",      // empty string
        ];

        for json in invalid_cases {
            let result: Result<Rate, _> = serde_json::from_str(json);
            assert!(result.is_err(), "Expected error for input: {}", json);
        }
    }

    #[test]
    fn test_deserialize_invalid_rate() {
        let invalid_cases = vec![
            "\"0/s\"",     // zero rate
            "\"-1/s\"",    // negative rate
            "\"abc/s\"",   // non-numeric rate
            "\"100.5/s\"", // decimal rate
        ];

        for json in invalid_cases {
            let result: Result<Rate, _> = serde_json::from_str(json);
            assert!(result.is_err(), "Expected error for input: {}", json);
        }
    }

    #[test]
    fn test_round_trip_serialization() {
        let test_cases = vec![
            Rate::PerSecond(NonZeroU32::new(1).unwrap()),
            Rate::PerSecond(NonZeroU32::new(100).unwrap()),
            Rate::PerMinute(NonZeroU32::new(60).unwrap()),
            Rate::PerHour(NonZeroU32::new(3600).unwrap()),
        ];

        for original in test_cases {
            let serialized = serde_json::to_string(&original).unwrap();
            let deserialized: Rate = serde_json::from_str(&serialized).unwrap();
            assert_eq!(original, deserialized);
        }
    }
}
