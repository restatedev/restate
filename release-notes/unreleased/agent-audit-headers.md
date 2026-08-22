# Agent Audit Context Headers

## New Feature

### What Changed

Added opt-in support for agent audit context propagation via well-known HTTP headers.

Two header constants are now defined in `restate_types::invocation::audit`:

- `x-restate-audit-triggered-by` — the human principal that triggered the call chain
- `x-restate-audit-conversation-id` — the human session/conversation that originated the chain

A new ingress config option `agent-audit` (default: `false`) controls whether these headers
are forwarded to handlers. When disabled, the headers are stripped at ingress to prevent
untrusted clients from injecting fake audit context.

### Why This Matters

When Restate orchestrates multi-agent AI workflows, there is no standard way to carry
the originating user identity and session across agent hops. These headers provide that
mechanism. All other audit fields (`agent_id`, `workflow_id`, `workflow_step`, `agent_type`)
are already derivable from Restate's existing invocation context.

### Impact on Users

- **Existing deployments**: No impact. The feature is disabled by default and the headers
  are stripped silently.
- **New deployments using agent workflows**: Enable the feature and attach the headers
  on every outbound call from your handlers.

### Migration Guidance

Enable in your Restate configuration:

```toml
[ingress]
agent-audit = true
```

Then in your handler, attach the headers on every outbound service call:

```python
AUDIT_TRIGGERED_BY = "x-restate-audit-triggered-by"
AUDIT_CONVERSATION_ID = "x-restate-audit-conversation-id"

await ctx.service_call(
    other_agent.handle,
    arg=payload,
    headers={
        AUDIT_TRIGGERED_BY: req.headers.get(AUDIT_TRIGGERED_BY),
        AUDIT_CONVERSATION_ID: req.headers.get(AUDIT_CONVERSATION_ID),
    }
)
```

The header values propagate unchanged from ingress all the way through the agent chain.
Use `ctx.invocation_id()` as `workflow_id` and `ctx.key()` as `agent_id` — these are
already provided by Restate at no extra cost.
