// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;

use anyhow::{Result, bail};
use cling::prelude::*;

use restate_admin_rest_model::rules::UpsertRuleRequest;
use restate_cli_util::c_success;
use restate_limiter::{AdaptiveConcurrency, Precondition, UserLimits};
use restate_types::Version;

use super::{fetch_rule, parse_pattern, upsert_one};
use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, DataFusionHttpClient};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_set")]
pub struct Set {
    /// Rule pattern, e.g. `*`, `scope1/*`, `scope1/foo/bar`
    pattern: String,

    /// Maximum concurrent running invocations (>= 1). On a new rule, omitting this means
    /// unlimited; on an existing rule it leaves the current limit unchanged.
    #[clap(long, conflicts_with = "unlimited")]
    concurrency: Option<NonZeroU32>,

    /// Set the rule to unlimited concurrency
    #[clap(long)]
    unlimited: bool,

    /// Scheduling weight (>= 1) for this rule's scope in the weighted
    /// round-robin scheduler: weight N receives N dispatch slots per cycle
    /// relative to weight-1 groups. On an existing rule, omitting this leaves
    /// the current weight unchanged. Requires the vqueues scheduler; only
    /// scope-level exact patterns are consulted.
    #[clap(long)]
    weight: Option<NonZeroU32>,

    /// Enable the adaptive (Gradient2) concurrency controller with defaults
    /// (min 4/partition, max 10000, tolerance 1.5x, smoothing 0.2). The
    /// learned limit replaces a static --concurrency; if both are set, the
    /// static limit takes precedence (instant rollback path). Recommended:
    /// pair with --adaptive-max set to the previous static cap.
    #[clap(long, conflicts_with = "no_adaptive")]
    adaptive: bool,

    /// Adaptive lower bound (>= 1). Implies --adaptive.
    #[clap(long, conflicts_with = "no_adaptive")]
    adaptive_min: Option<NonZeroU32>,

    /// Adaptive upper bound (>= 1). Implies --adaptive.
    #[clap(long, conflicts_with = "no_adaptive")]
    adaptive_max: Option<NonZeroU32>,

    /// Adaptive tolerance as a factor, e.g. 1.5 (stored as permille).
    /// Implies --adaptive.
    #[clap(long, conflicts_with = "no_adaptive")]
    adaptive_tolerance: Option<f64>,

    /// Adaptive smoothing factor, e.g. 0.2 (stored as permille).
    /// Implies --adaptive.
    #[clap(long, conflicts_with = "no_adaptive")]
    adaptive_smoothing: Option<f64>,

    /// Remove the adaptive controller from the rule.
    #[clap(long)]
    no_adaptive: bool,

    /// Description for the rule
    #[clap(long)]
    description: Option<String>,

    /// Create the rule in a disabled (parked) state. Only valid for new rules;
    /// use `restate rules disable` to disable an existing rule.
    #[clap(long)]
    disabled: bool,
}

/// Converts a factor flag (e.g. 1.5) to permille (1500), validating range.
fn factor_to_permille(name: &str, value: Option<f64>, max: f64) -> Result<Option<NonZeroU32>> {
    match value {
        None => Ok(None),
        Some(v) if v.is_finite() && v > 0.0 && v <= max => {
            Ok(NonZeroU32::new((v * 1000.0).round() as u32))
        }
        Some(v) => bail!("--{name} must be a factor in (0, {max}] (got {v})"),
    }
}

pub async fn run_set(State(env): State<CliEnv>, opts: &Set) -> Result<()> {
    let pattern = parse_pattern(&opts.pattern)?;
    // Any adaptive-* flag implies --adaptive
    let wants_adaptive = opts.adaptive
        || opts.adaptive_min.is_some()
        || opts.adaptive_max.is_some()
        || opts.adaptive_tolerance.is_some()
        || opts.adaptive_smoothing.is_some();
    let adaptive_flags = if wants_adaptive {
        Some(AdaptiveConcurrency {
            min: opts.adaptive_min,
            max: opts.adaptive_max,
            tolerance_permille: factor_to_permille(
                "adaptive-tolerance",
                opts.adaptive_tolerance,
                1000.0,
            )?,
            smoothing_permille: factor_to_permille(
                "adaptive-smoothing",
                opts.adaptive_smoothing,
                1.0,
            )?,
        })
    } else {
        None
    };
    let canonical = pattern.to_string();

    let sql_client = DataFusionHttpClient::new(&env).await?;
    let current = fetch_rule(&sql_client, &canonical).await?;
    let was_create = current.is_none();

    let request = match &current {
        None => UpsertRuleRequest {
            pattern,
            limits: UserLimits::new(if opts.unlimited {
                None
            } else {
                opts.concurrency
            })
            .with_scheduling_weight(opts.weight)
            .with_adaptive_concurrency(adaptive_flags.clone()),
            description: opts.description.clone(),
            disabled: opts.disabled,
            precondition: Precondition::DoesNotExist,
        },
        Some(rule) => {
            if opts.disabled {
                bail!(
                    "Rule '{canonical}' already exists. Use `restate rules disable` to disable it."
                );
            }
            let concurrency = if opts.unlimited {
                None
            } else {
                opts.concurrency.or_else(|| rule.concurrency())
            };
            let description = opts
                .description
                .clone()
                .or_else(|| rule.description.clone());
            let scheduling_weight = opts.weight.or_else(|| rule.scheduling_weight());
            // Preserve-on-omit: without adaptive flags the existing adaptive
            // config survives; --no-adaptive clears it; adaptive flags replace it.
            let adaptive = if opts.no_adaptive {
                None
            } else {
                adaptive_flags
                    .clone()
                    .or_else(|| rule.adaptive_concurrency())
            };
            UpsertRuleRequest {
                pattern,
                limits: UserLimits::new(concurrency)
                    .with_scheduling_weight(scheduling_weight)
                    .with_adaptive_concurrency(adaptive),
                description,
                disabled: rule.disabled,
                precondition: Precondition::Matches(Version::from(rule.version)),
            }
        }
    };

    let client = AdminClient::new(&env).await?;
    let result = upsert_one(
        &client,
        request,
        &format!("Rule '{canonical}' was modified concurrently; please re-run."),
    )
    .await?;

    let verb = if was_create { "Created" } else { "Updated" };
    match result {
        Some(rule) => c_success!("{verb} rule '{canonical}' (version {})", rule.version),
        None => c_success!("{verb} rule '{canonical}'"),
    }
    Ok(())
}
