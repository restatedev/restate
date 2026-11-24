// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Restart or suspend, after a shutdown: the six rules of 10 §10.5, in order.

use restate_worker_api::invoker::explicit_recovery::RestartRule;

/// What the runner knew when the attempt ended with a shutdown.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ShutdownFacts {
    /// The runtime closed its side because the attempt was silent for the inactivity timeout.
    /// False when the SDK sent `ShutdownMessage` without being asked.
    pub closed_for_inactivity: bool,
    /// A `ShutdownMessage` came after the close.
    pub answered: bool,
    /// `ShutdownMessage.processed_notifications`.
    pub processed: u32,
    /// Notifications the runtime sent in the attempt.
    pub sent: u32,
    /// A notification arrived for the attempt after the close, and could not be sent.
    pub notified_after_close: bool,
    /// The SDK sent something other than its last message after the close.
    pub crossed: bool,
    /// An external result is outstanding.
    pub external_outstanding: bool,
    /// Forwards taken from the channel: what the attempt reports in a suspend (D34).
    pub received_forwards: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Decision {
    Restart(RestartRule),
    Suspend { received_notifications: u32 },
    Violation(String),
}

pub(crate) fn decide(facts: &ShutdownFacts) -> Decision {
    if !facts.closed_for_inactivity {
        return Decision::Restart(RestartRule::OwnReason);
    }
    if !facts.answered {
        return Decision::Restart(RestartRule::NoAnswer);
    }
    if facts.processed > facts.sent {
        return Decision::Violation(format!(
            "ShutdownMessage says {} notifications were processed, but {} were sent",
            facts.processed, facts.sent
        ));
    }
    if facts.processed < facts.sent || facts.notified_after_close {
        return Decision::Restart(RestartRule::Unprocessed);
    }
    if facts.crossed {
        return Decision::Restart(RestartRule::Crossed);
    }
    if !facts.external_outstanding {
        return Decision::Restart(RestartRule::NothingOutstanding);
    }
    Decision::Suspend {
        received_notifications: facts.received_forwards,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_six_rules_in_order() {
        // Rule 6: silent, answered, everything processed, waiting on something.
        let suspend = ShutdownFacts {
            closed_for_inactivity: true,
            answered: true,
            processed: 2,
            sent: 2,
            external_outstanding: true,
            received_forwards: 3,
            ..Default::default()
        };
        assert_eq!(
            decide(&suspend),
            Decision::Suspend {
                received_notifications: 3
            }
        );

        let cases = [
            (
                ShutdownFacts {
                    closed_for_inactivity: false,
                    ..suspend
                },
                Decision::Restart(RestartRule::OwnReason),
            ),
            (
                ShutdownFacts {
                    answered: false,
                    // Rule 2 comes before the processed count is even looked at.
                    processed: 9,
                    ..suspend
                },
                Decision::Restart(RestartRule::NoAnswer),
            ),
            (
                ShutdownFacts {
                    processed: 1,
                    ..suspend
                },
                Decision::Restart(RestartRule::Unprocessed),
            ),
            (
                ShutdownFacts {
                    notified_after_close: true,
                    ..suspend
                },
                Decision::Restart(RestartRule::Unprocessed),
            ),
            (
                ShutdownFacts {
                    crossed: true,
                    ..suspend
                },
                Decision::Restart(RestartRule::Crossed),
            ),
            (
                ShutdownFacts {
                    external_outstanding: false,
                    ..suspend
                },
                Decision::Restart(RestartRule::NothingOutstanding),
            ),
        ];
        for (facts, expected) in cases {
            assert_eq!(decide(&facts), expected, "{facts:?}");
        }

        // Processed more than sent: a violation.
        assert!(matches!(
            decide(&ShutdownFacts {
                processed: 3,
                ..suspend
            }),
            Decision::Violation(_)
        ));
    }
}
