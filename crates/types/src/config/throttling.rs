use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::rate::Rate;

/// # Throttling options
///
/// Token-bucket throttling options.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ThrottlingOptions {
    /// # Refill rate
    ///
    /// The rate at which the tokens are replenished.
    ///
    /// Syntax: `<rate>/<unit>` where `<unit>` is `s|sec|second`, `m|min|minute`, or `h|hr|hour`.
    /// unit defaults to per second if not specified.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub rate: Rate,

    /// # Burst capacity
    ///
    /// The maximum number of tokens the bucket can hold.
    /// Default to the rate value if not specified.
    pub capacity: Option<NonZeroU32>,
}

impl From<ThrottlingOptions> for gardal::Limit {
    fn from(options: ThrottlingOptions) -> Self {
        use gardal::Limit;

        let mut limit = match options.rate {
            Rate::Second(rate) => Limit::per_second(rate),
            Rate::Minute(rate) => Limit::per_minute(rate),
            Rate::Hour(rate) => Limit::per_hour(rate),
        };

        if let Some(capacity) = options.capacity {
            limit = limit.with_burst(capacity);
        }

        limit
    }
}
