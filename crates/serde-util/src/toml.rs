// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Apply TOML patch on top of an existing serializable struct
///
/// The base struct is serialized and extended with the provided patch. Values
/// in the patch take precedence. Invalid keys in the patch are ignored. The
/// resulting table is then deserialized into a fresh T.
///
/// WARNING: non-serialized fields in the base T are NOT preserved.
///
/// Returns an error if:
/// - The base struct cannot be serialized to TOML
/// - The patch string is not valid TOML syntax
/// - The base struct's TOML representation cannot be parsed back
/// - The final patched TOML cannot be deserialized back to type T
pub fn patch_toml<T>(base: &T, patch_toml: &str) -> Result<T, String>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone,
{
    let patch = toml::from_str::<toml::Table>(patch_toml)
        .map_err(|e| format!("Invalid patch TOML syntax: {}", e))?;

    let base_str = toml::to_string(base)
        .map_err(|e| format!("Failed to serialize base struct to TOML: {}", e))?;

    let mut derived = toml::from_str::<toml::Table>(&base_str)
        .map_err(|e| format!("Failed to parse base struct TOML: {}", e))?;

    derived.extend(patch);

    let final_toml =
        toml::to_string(&derived).map_err(|e| format!("Failed to serialize final TOML: {}", e))?;

    toml::from_str::<T>(&final_toml).map_err(|e| {
        format!(
            "Failed to deserialize final TOML back to target type: {}",
            e
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestConfig {
        name: String,
        count: u32,
        enabled: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        optional: Option<String>,
        #[serde(skip)]
        internal: String,
    }

    #[test]
    fn test_patch_toml_basic() {
        let base = TestConfig {
            name: "original".to_string(),
            count: 10,
            enabled: false,
            optional: Some("value".to_string()),
            internal: "internal_value".to_string(),
        };

        let patch = r#"
            name = "patched"
            enabled = true
        "#;

        let result = patch_toml(&base, patch).unwrap();

        assert_eq!(result.name, "patched");
        assert_eq!(result.count, 10); // unchanged
        assert_eq!(result.enabled, true);
        assert_eq!(result.optional, Some("value".to_string())); // unchanged
        assert_eq!(result.internal, String::new()); // lost
    }

    #[test]
    fn test_patch_toml_invalid_patch() {
        let base = TestConfig {
            name: "original".to_string(),
            count: 10,
            enabled: false,
            optional: None,
            internal: "".to_string(),
        };

        let invalid_patch = "invalid toml syntax {";
        let result = patch_toml(&base, invalid_patch);

        // Should return an error for invalid TOML syntax
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid patch TOML syntax"));
    }

    #[test]
    fn test_patch_toml_empty_patch() {
        let base = TestConfig {
            name: "original".to_string(),
            count: 10,
            enabled: false,
            optional: None,
            internal: "".to_string(),
        };

        let empty_patch = "";
        let result = patch_toml(&base, empty_patch).unwrap();

        // Should return original unchanged (empty patch is valid TOML)
        assert_eq!(result.name, base.name);
        assert_eq!(result.count, base.count);
        assert_eq!(result.enabled, base.enabled);
        assert_eq!(result.optional, base.optional);
        assert_eq!(result.internal, String::new()); // lost due to serialization
    }

    #[test]
    fn test_patch_toml_all_fields() {
        let base = TestConfig {
            name: "original".to_string(),
            count: 10,
            enabled: false,
            optional: None,
            internal: "".to_string(),
        };

        let patch = r#"
            name = "completely_new"
            count = 99
            enabled = true
            optional = "new_optional"
        "#;

        let result = patch_toml(&base, patch).unwrap();

        assert_eq!(result.name, "completely_new");
        assert_eq!(result.count, 99);
        assert_eq!(result.enabled, true);
        assert_eq!(result.optional, Some("new_optional".to_string()));
        assert_eq!(result.internal, String::new()); // lost
    }

    #[test]
    fn test_retry_policy_patch() {
        use restate_types::retries::RetryPolicy;
        use std::num::NonZeroUsize;

        // Base policy: Exponential with specific settings
        let base_policy = RetryPolicy::Exponential {
            initial_interval: humantime::Duration::from(std::time::Duration::from_millis(100)),
            factor: 2.0,
            max_attempts: Some(NonZeroUsize::new(5).unwrap()),
            max_interval: Some(humantime::Duration::from(std::time::Duration::from_secs(
                30,
            ))),
        };

        // Test 1: Valid TOML that overrides specific fields
        let patch1 = r#"
                factor = 3.0
                max-attempts = 10
            "#;
        let result1 = patch_toml(&base_policy, patch1).unwrap();
        match result1 {
            RetryPolicy::Exponential {
                factor,
                max_attempts,
                initial_interval,
                max_interval,
            } => {
                assert_eq!(factor, 3.0);
                assert_eq!(max_attempts, Some(NonZeroUsize::new(10).unwrap()));
                // These should remain unchanged from base
                assert_eq!(
                    initial_interval,
                    humantime::Duration::from(std::time::Duration::from_millis(100))
                );
                assert_eq!(
                    max_interval,
                    Some(humantime::Duration::from(std::time::Duration::from_secs(
                        30
                    )))
                );
            }
            _ => panic!("Expected Exponential policy"),
        }

        // Test 2: TOML that changes policy type entirely
        let patch2 = r#"
                type = "fixed-delay"
                interval = "500ms"
                max-attempts = 3
            "#;
        let result2 = patch_toml(&base_policy, patch2).unwrap();
        match result2 {
            RetryPolicy::FixedDelay {
                interval,
                max_attempts,
            } => {
                assert_eq!(
                    interval,
                    humantime::Duration::from(std::time::Duration::from_millis(500))
                );
                assert_eq!(max_attempts, Some(NonZeroUsize::new(3).unwrap()));
            }
            _ => panic!("Expected FixedDelay policy"),
        }

        // Test 3: Invalid TOML syntax should return error
        let invalid_toml = r#"invalid toml syntax {"#; // Missing closing brace and invalid structure
        let result3 = patch_toml(&base_policy, invalid_toml);
        assert!(result3.is_err());

        // Test 4: Valid TOML with non-matching keys should succeed (ignored)
        let non_matching_keys = r#"
            factor = 3.0
            non_existent_field = "should_be_ignored"
            another_invalid_key = 42
        "#;
        let result4 = patch_toml(&base_policy, non_matching_keys).unwrap();
        match result4 {
            RetryPolicy::Exponential { factor, .. } => {
                assert_eq!(factor, 3.0); // Valid field should be applied
                // Invalid fields should be ignored
            }
            _ => panic!("Expected Exponential policy"),
        }

        // Test 5: Empty patch should return success unchanged
        let empty_patch = "";
        let result5 = patch_toml(&base_policy, empty_patch).unwrap();
        assert_eq!(result5, base_policy);

        // Test 6: Valid patch with only one field
        let single_field = r#"factor = 1.5"#;
        let result6 = patch_toml(&base_policy, single_field).unwrap();
        match result6 {
            RetryPolicy::Exponential {
                factor,
                max_attempts,
                initial_interval,
                max_interval,
            } => {
                assert_eq!(factor, 1.5);
                // These should remain unchanged from base
                assert_eq!(max_attempts, Some(NonZeroUsize::new(5).unwrap()));
                assert_eq!(
                    initial_interval,
                    humantime::Duration::from(std::time::Duration::from_millis(100))
                );
                assert_eq!(
                    max_interval,
                    Some(humantime::Duration::from(std::time::Duration::from_secs(
                        30
                    )))
                );
            }
            _ => panic!("Expected Exponential policy"),
        }

        // Test 7: Test changing to None policy
        let to_none_patch = r#"type = "none""#;
        let result7 = patch_toml(&base_policy, to_none_patch).unwrap();
        match result7 {
            RetryPolicy::None => {
                // Success - changed to None policy
            }
            _ => panic!("Expected None policy"),
        }

        // Test 8: Test that invalid values for valid keys cause deserialization error
        let invalid_value_patch = r#"factor = "not_a_number""#;
        let result8 = patch_toml(&base_policy, invalid_value_patch);
        assert!(result8.is_err());
        assert!(
            result8
                .unwrap_err()
                .contains("Failed to deserialize final TOML")
        );
    }
}
