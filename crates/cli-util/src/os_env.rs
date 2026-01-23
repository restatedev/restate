// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(not(any(test, feature = "test-util")))]
mod os_env {
    /// Wrapper over the OS environment variables
    #[derive(Default)]
    pub struct OsEnv<'a> {
        _marker: std::marker::PhantomData<&'a ()>,
    }

    impl OsEnv<'_> {
        // Retrieves a environment variable from the os or from a table if in testing mode
        #[inline]
        pub fn get<K: AsRef<str>>(&self, key: K) -> Option<String> {
            std::env::var(key.as_ref()).ok()
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
mod test_os_env {
    use std::collections::HashMap;

    /// Wrapper over the OS environment variables that uses a hashmap to enable testing.
    #[derive(Default)]
    pub struct OsEnv<'a> {
        /// Environment variable mocks
        pub env: HashMap<&'a str, String>,
    }

    impl<'a> OsEnv<'a> {
        // Retrieves a environment variable from the os or from a table if in testing mode
        pub fn get<K: AsRef<str>>(&self, key: K) -> Option<String> {
            self.env.get(key.as_ref()).map(ToString::to_string)
        }

        pub fn insert(&mut self, k: &'a str, v: String) -> Option<String> {
            self.env.insert(k, v)
        }

        pub fn clear(&mut self) {
            self.env.clear();
        }
    }
}

#[cfg(any(test, feature = "test-util"))]
pub use test_os_env::OsEnv;

#[cfg(not(any(test, feature = "test-util")))]
pub use os_env::OsEnv;
