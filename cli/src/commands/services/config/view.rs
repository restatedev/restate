// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use anyhow::Result;
use cling::prelude::*;
use indoc::indoc;
use serde::Serialize;
use serde_json::Value;

use restate_types::invocation::ServiceType;
use restate_types::schema::invocation_target::OnMaxAttempts;
use restate_util_time::DurationExt;

use crate::cli_env::CliEnv;
use crate::clients::{AdminClient, AdminClientInterface};
use crate::ui::fmt::{Field, Formatter, ListItem, OutputFormatter};
use crate::ui::service_handlers::service_type_field;

// TODO we could infer this text from the OpenAPI docs!
pub(super) const PUBLIC_DESCRIPTION: &str = indoc! {
    "Whether the service is publicly available or not.
    If true, the service can be invoked through the ingress.
    If false, the service can be invoked only from another Restate service."
};
pub(super) const IDEMPOTENCY_RETENTION: &str = indoc! {
    "The retention duration of idempotent requests for this service.
    The retention period starts once the invocation completes (with either success or failure).
    After the retention period, the invocation response and the idempotency key will be forgotten."
};
pub(super) const WORKFLOW_RETENTION: &str = indoc! {
    "The retention duration of workflows.
    The retention period starts once the invocation completes (with either success or failure).
    After the retention period, the invocation response together with the workflow state and promises will be forgotten."
};
pub(super) const JOURNAL_RETENTION: &str = indoc! {
    "The journal retention.
    The retention period starts once the invocation completes (with either success or failure).

    In case the invocation has an idempotency key, the `idempotency_retention` caps the maximum `journal_retention` time.
    In case the invocation targets a workflow handler, the `workflow_completion_retention` caps the maximum `journal_retention` time."
};
pub(super) const INACTIVITY_TIMEOUT: &str = indoc! {
    "This timer guards against stalled service/handler invocations. Once it expires,
    Restate triggers a graceful termination by asking the service invocation to
    suspend (which preserves intermediate progress).

    The 'abort timeout' is used to abort the invocation, in case it doesn't react to
    the request to suspend.

    This overrides the default inactivity timeout set in invoker options."
};
pub(super) const ABORT_TIMEOUT: &str = indoc! {
    "This timer guards against stalled service/handler invocations that are supposed to terminate.
    The abort timeout is started after the 'inactivity timeout' has expired and the
    service/handler invocation has been asked to gracefully terminate.
    Once the timer expires, it will abort the service/handler invocation.

    This timer potentially **interrupts** user code. If the user code needs longer to
    gracefully terminate, then this value needs to be set accordingly.

    This overrides the default abort timeout set in invoker options."
};
pub(super) const STATE_PRELOAD_POLICY: &str = indoc! {
    "Which state is preloaded (sent eagerly) at the start of an invocation:
    'eager' preloads all state, 'lazy' preloads none, 'selective (...)' preloads only the listed keys.
    This is relevant only for Workflows and Virtual Objects."
};
pub(super) const RETRY_POLICY: &str = indoc! {
    "Retry policy to use for transient errors. The next retry interval is calculated as
    initial_interval * (exponentiation_factor ^ attempt), capped at max_interval.

    Max attempts: Maximum number of retry attempts before giving up (infinite if unset).
    On max attempts: What to do when max attempts are reached (Pause or Kill)."
};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_view")]
pub struct View {
    /// Service name
    service: String,
}

pub async fn run_view(State(env): State<CliEnv>, opts: &View) -> Result<()> {
    view(&env, opts).await
}

async fn view(env: &CliEnv, opts: &View) -> Result<()> {
    let client = AdminClient::new(env).await?;
    let service = client.get_service(&opts.service).await?.into_body().await?;

    let mut f = Formatter::new();
    f.detail(
        "service",
        [
            ("name", Field::new(service.name.as_str())),
            ("service_type", service_type_field(&service.ty)),
        ],
    );

    let retry = &service.retry_policy;
    let mut options = vec![
        ConfigOption::new("public", "Public", service.public, PUBLIC_DESCRIPTION),
        ConfigOption::new(
            "idempotency_retention",
            "Idempotent requests retention",
            service.idempotency_retention.friendly().to_string(),
            IDEMPOTENCY_RETENTION,
        ),
    ];
    if service.ty == ServiceType::Workflow
        && let Some(retention) = service.workflow_completion_retention
    {
        options.push(ConfigOption::new(
            "workflow_completion_retention",
            "Workflow retention time",
            retention.friendly().to_string(),
            WORKFLOW_RETENTION,
        ));
    }
    options.extend([
        ConfigOption::new(
            "journal_retention",
            "Journal retention",
            service.journal_retention.map(|d| d.friendly().to_string()),
            JOURNAL_RETENTION,
        ),
        ConfigOption::new(
            "inactivity_timeout",
            "Inactivity timeout",
            service.inactivity_timeout.friendly().to_string(),
            INACTIVITY_TIMEOUT,
        ),
        ConfigOption::new(
            "abort_timeout",
            "Abort timeout",
            service.abort_timeout.friendly().to_string(),
            ABORT_TIMEOUT,
        ),
        ConfigOption::new(
            "state_preload_policy",
            "State preload policy",
            service.state_preload_policy.to_string(),
            STATE_PRELOAD_POLICY,
        ),
        ConfigOption {
            option: "retry_policy",
            value: serde_json::to_value(retry)?,
            description: RETRY_POLICY,
            label: "Retry policy",
            display: retry_policy_summary(
                retry.max_attempts.map(NonZeroUsize::get),
                Some(retry.on_max_attempts),
                Some(retry.initial_interval),
                Some(retry.exponentiation_factor),
                retry.max_interval,
            ),
        },
    ]);
    f.title("⚙️", "Options");
    f.list("options", &options)?;

    // Handler-level overrides of the options above.
    let mut overrides = Vec::new();
    let mut handlers: Vec<_> = service.handlers.values().collect();
    handlers.sort_by(|a, b| a.name.cmp(&b.name));
    for handler in handlers {
        let mut add = |option, label, value: Value, display: String| {
            overrides.push(HandlerOverride {
                handler: handler.name.clone(),
                option,
                value,
                label,
                display,
            })
        };
        if let Some(d) = handler.idempotency_retention {
            let d = d.friendly().to_string();
            add(
                "idempotency_retention",
                "Idempotent requests retention",
                Value::from(d.clone()),
                d,
            );
        }
        if let Some(d) = handler.journal_retention {
            let d = d.friendly().to_string();
            add(
                "journal_retention",
                "Journal retention",
                Value::from(d.clone()),
                d,
            );
        }
        if let Some(d) = handler.inactivity_timeout {
            let d = d.friendly().to_string();
            add(
                "inactivity_timeout",
                "Inactivity timeout",
                Value::from(d.clone()),
                d,
            );
        }
        if let Some(d) = handler.abort_timeout {
            let d = d.friendly().to_string();
            add("abort_timeout", "Abort timeout", Value::from(d.clone()), d);
        }
        if let Some(policy) = &handler.state_preload_policy {
            let policy = policy.to_string();
            add(
                "state_preload_policy",
                "State preload policy",
                Value::from(policy.clone()),
                policy,
            );
        }
        if handler.public != service.public {
            add(
                "public",
                "Public",
                Value::from(handler.public),
                handler.public.to_string(),
            );
        }
        let retry = &handler.retry_policy;
        if !is_retry_policy_empty(retry) {
            add(
                "retry_policy",
                "Retry policy",
                serde_json::to_value(retry)?,
                retry_policy_summary(
                    retry.max_attempts.map(NonZeroUsize::get),
                    retry.on_max_attempts,
                    retry.initial_interval,
                    retry.exponentiation_factor,
                    retry.max_interval,
                ),
            );
        }
    }
    if !overrides.is_empty() {
        f.title("🔌", "Handler Overrides");
    }
    f.list("handler_overrides", &overrides)?;
    f.finish()
}

/// A service configuration option: its value, and its documentation as detail lines.
#[derive(Serialize)]
struct ConfigOption {
    option: &'static str,
    value: Value,
    description: &'static str,
    #[serde(skip)]
    label: &'static str,
    #[serde(skip)]
    display: String,
}

impl ConfigOption {
    fn new(
        option: &'static str,
        label: &'static str,
        value: impl Into<Value>,
        description: &'static str,
    ) -> Self {
        let value = value.into();
        let display = match &value {
            Value::Null => "<UNSET>".to_owned(),
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        Self {
            option,
            value,
            description,
            label,
            display,
        }
    }
}

impl ListItem for ConfigOption {
    const HEADERS: &'static [&'static str] = &["option", "value"];

    fn columns(&self) -> Vec<Field> {
        vec![Field::new(self.label), Field::new(self.display.as_str())]
    }

    fn details(&self) -> Vec<String> {
        self.description.lines().map(str::to_owned).collect()
    }
}

/// A handler-level override of a service configuration option.
#[derive(Serialize)]
struct HandlerOverride {
    handler: String,
    option: &'static str,
    value: Value,
    #[serde(skip)]
    label: &'static str,
    #[serde(skip)]
    display: String,
}

impl ListItem for HandlerOverride {
    const HEADERS: &'static [&'static str] = &["handler", "option", "value"];

    fn columns(&self) -> Vec<Field> {
        vec![
            Field::new(self.handler.as_str()),
            Field::new(self.label),
            Field::new(self.display.as_str()),
        ]
    }
}

/// A retry policy in one line, e.g. `70 attempts, then pause · 500ms × 2, up to 1m`.
/// Unset parts are left out (handler overrides set only some).
fn retry_policy_summary(
    max_attempts: Option<usize>,
    on_max_attempts: Option<OnMaxAttempts>,
    initial_interval: Option<std::time::Duration>,
    exponentiation_factor: Option<f32>,
    max_interval: Option<std::time::Duration>,
) -> String {
    let mut attempts = Vec::new();
    match max_attempts {
        Some(n) => attempts.push(format!("{n} attempts")),
        None => attempts.push("unlimited attempts".to_owned()),
    }
    if let Some(on_max) = on_max_attempts {
        attempts.push(format!("then {}", format!("{on_max:?}").to_lowercase()));
    }
    let mut interval = Vec::new();
    if let Some(initial) = initial_interval {
        interval.push(initial.friendly().to_string());
    }
    if let Some(factor) = exponentiation_factor {
        interval.push(format!("× {factor}"));
    }
    let mut interval = interval.join(" ");
    if let Some(max) = max_interval {
        if !interval.is_empty() {
            interval.push_str(", ");
        }
        interval.push_str(&format!("up to {}", max.friendly()));
    }
    let mut parts = vec![attempts.join(", ")];
    if !interval.is_empty() {
        parts.push(interval);
    }
    parts.join(" · ")
}

fn is_retry_policy_empty(
    retry_policy: &restate_types::schema::service::HandlerRetryPolicyMetadata,
) -> bool {
    retry_policy.initial_interval.is_none()
        && retry_policy.exponentiation_factor.is_none()
        && retry_policy.max_attempts.is_none()
        && retry_policy.max_interval.is_none()
        && retry_policy.on_max_attempts.is_none()
}
