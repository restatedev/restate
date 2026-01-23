// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tiny_gradient::{Gradient, GradientStr};

pub const RESTATE_LOGO_ART: &str = r###"
     →↓→↓
    →↓→↓→↓→→→               →→→→
    ↓→↓→↓→↓↓→↓→→           →↓↓→↓→↓→
    →↓→↓→↓→↓→↓↓→↓→↓       →↓→↓→↓→↓→↓→
    →↓→↓→↓→↓→↓→↓→↓→↓→→      →↓→↓→↓→↓→↓→→
    →↓→↓→↓→ →↓→↓→↓→↓→↓→↓→      →↓→↓→↓→↓↓→↓→↗
    →↓→↓→↓→    →↓→↓→↓→↓→↓→↓→     →→↓→↓→↓→↓↓→↓→
    →↓→↓→↓→       →↓→↓→↓→↓→↓→↓→     →→↓→↓→↓→↓→↓→→
    →↓→↓→↓→          →↓→↓→↓→↓→↓→↓→     →→↓→↓→↓→↓↓→↓→
    →↓→↓→↓→             →↓→↓→↓→↓→↓→       →→↓→↓→↓→↓→↓
    →↓→↓→↓→             →↓→↓→↓→↓→↓→      →→↓→↓→↓→↓→↓→
    →↓→↓→↓→          →↓→↓→↓→↓→↓→↓      →↓→↓→↓→↓→↓→→
    →↓→↓→↓→       →→↓→↓→↓→↓→↓→→     →↓→↓→↓→↓→↓→→
    →↓→↓→↓→      →↓→↓→↓→↓→↓→     →↓→↓→↓→↓→↓→↓
    →↓→↓→↓→     →↓→↓→↓→↓→     →↓→↓→↓→↓→↓→↓→
    →↓→↓→↓→      ↓→↓→→      →↓→↓→↓→↓→↓→→
    →↓→↓→↓→               →↓→↓→↓→↓→↓→
    →↓→↓→↓→                →↓→↓→↓→
    ↓→↓→↓→↓
     →↓→→
"###;

pub fn render_restate_logo(ansi_color: bool) -> String {
    if ansi_color {
        RESTATE_LOGO_ART.gradient(Gradient::Mind).to_string()
    } else {
        RESTATE_LOGO_ART.to_owned()
    }
}
