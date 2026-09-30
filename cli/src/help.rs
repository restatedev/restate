// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Building blocks for the text of `--help`.

/// A help section heading, styled like clap's own (e.g. `Options:`): bold and underlined.
/// Clap strips the escape codes when colors are off.
macro_rules! heading {
    ($title:literal) => {
        concat!("\x1b[1m\x1b[4m", $title, "\x1b[0m")
    };
}

/// A command's `after_help`: an `Examples:` section, one command per line (a line starting
/// with `#` is a comment), and/or a `Learn more:` section with a link to the docs.
macro_rules! after_help {
    (examples: [$($example:literal),+ $(,)?] $(,)?) => {
        concat!(heading!("Examples:"), $("\n  ", $example),+)
    };
    (learn_more: $url:literal $(,)?) => {
        concat!(heading!("Learn more:"), "\n  ", $url)
    };
    (examples: [$($example:literal),+ $(,)?], learn_more: $url:literal $(,)?) => {
        concat!(
            after_help!(examples: [$($example),+]),
            "\n\n",
            after_help!(learn_more: $url)
        )
    };
}

/// [`heading!`] for a title only known at runtime.
pub(crate) fn heading(title: &str) -> String {
    format!("\x1b[1m\x1b[4m{title}\x1b[0m")
}
