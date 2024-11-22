// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate proc_macro;

mod tc_test;

use proc_macro::TokenStream;

/// Run tests within task-center
///
/// `#[restate_core::test(_args_of_tokio_test)]`
///
/// ```no_run
/// #[restate_core::test(start_paused = true)]`
/// async fn test_name() {
///    TaskCenter::current();
/// }
/// ```
///
#[proc_macro_attribute]
pub fn test(args: TokenStream, item: TokenStream) -> TokenStream {
    tc_test::test(args.into(), item.into(), true).into()
}
