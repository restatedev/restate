// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::de::Error;
use serde::{Deserialize, Deserializer, Serializer};
use serde_with::{DeserializeAs, SerializeAs};

/// Serializable/Deserializable duration to use with serde_with.
///
/// When serializing the humantime format is used.
///
/// When deserializing, the following formats are accepted:
///
/// * ISO8601 durations
/// * Humantime durations
pub struct DurationString;

impl<'de> DeserializeAs<'de, std::time::Duration> for DurationString {
    fn deserialize_as<D>(deserializer: D) -> Result<std::time::Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with('P') {
            Ok(iso8601::duration(&s).map_err(Error::custom)?.into())
        } else {
            humantime::parse_duration(&s).map_err(Error::custom)
        }
    }
}

impl SerializeAs<std::time::Duration> for DurationString {
    fn serialize_as<S>(source: &std::time::Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&humantime::Duration::from(*source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::{Deserialize, Serialize};

    use serde_with::serde_as;

    #[serde_as]
    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    struct MyDuration(#[serde_as(as = "DurationString")] std::time::Duration);

    #[test]
    fn serialize_humantime() {
        let d = std::time::Duration::from_secs(60 * 23);

        let result_string =
            serde_json::from_str::<String>(&serde_json::to_string(&MyDuration(d)).unwrap())
                .unwrap();

        assert_eq!(result_string, humantime::Duration::from(d).to_string());
    }

    #[test]
    fn deserialize_iso8601() {
        let d = std::time::Duration::from_secs(10);

        assert_eq!(
            serde_json::from_value::<MyDuration>(serde_json::Value::String("PT10S".to_owned()))
                .unwrap()
                .0,
            d
        );
    }

    #[test]
    fn deserialize_humantime() {
        let d = std::time::Duration::from_secs(60 * 23);

        assert_eq!(
            serde_json::from_value::<MyDuration>(serde_json::Value::String(
                humantime::Duration::from(d).to_string()
            ))
            .unwrap()
            .0,
            d
        );
    }
}
