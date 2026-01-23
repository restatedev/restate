// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::alphabet;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};

// We have this custom configuration for padding because Node's base64url implementation will omit padding,
// but the spec https://datatracker.ietf.org/doc/html/rfc4648#section-5 doesn't specify that padding MUST be omitted.
// Hence, Other implementations might include it, so we keep this relaxed.
pub const INDIFFERENT_PAD: GeneralPurposeConfig = GeneralPurposeConfig::new()
    .with_decode_padding_mode(DecodePaddingMode::Indifferent)
    .with_encode_padding(false);
pub const URL_SAFE: GeneralPurpose = GeneralPurpose::new(&alphabet::URL_SAFE, INDIFFERENT_PAD);
