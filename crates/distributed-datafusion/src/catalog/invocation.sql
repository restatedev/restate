CREATE VIEW restate.public.sys_invocation AS SELECT
    ss.id,
    ss.vqueue_id,
    ss.target,
    ss.target_service_name,
    ss.target_service_key,
    ss.target_handler_name,
    ss.target_service_ty,
    ss.scope,
    ss.limit_key,
    ss.idempotency_key,
    ss.invoked_by,
    ss.invoked_by_service_name,
    ss.invoked_by_id,
    ss.invoked_by_subscription_id,
    ss.invoked_by_target,
    ss.restarted_from,
    ss.pinned_deployment_id,
    ss.pinned_service_protocol_version,
    ss.trace_id,
    ss.journal_size,
    ss.journal_commands_size,
    ss.created_at,
    ss.created_using_restate_version,
    ss.modified_at,
    ss.inboxed_at,
    ss.scheduled_at,
    ss.scheduled_start_at,
    ss.running_at,
    ss.completed_at,
    ss.completion_retention,
    ss.journal_retention,
    ss.suspended_waiting_for_completions,
    ss.suspended_waiting_for_signals,
    ss.suspended_waiting_future_json,
    sis.retry_count,
    sis.last_start_at,
    sis.next_retry_at,
    sis.last_attempt_deployment_id,
    sis.last_attempt_server,
    sis.last_failure,
    sis.last_failure_error_code,
    sis.last_failure_related_entry_index,
    sis.last_failure_related_entry_name,
    sis.last_failure_related_entry_type,
    sis.last_failure_related_command_index,
    sis.last_failure_related_command_name,
    sis.last_failure_related_command_type,
    sis.last_awaiting_on_future_json,
    arrow_cast(CASE
        WHEN ss.status = 'inboxed' THEN 'pending'
        WHEN ss.status = 'scheduled' THEN 'scheduled'
        WHEN ss.status = 'completed' THEN 'completed'
        WHEN ss.status = 'suspended' THEN 'suspended'
        WHEN ss.status = 'paused' THEN 'paused'
        WHEN sis.in_flight THEN 'running'
        WHEN ss.status = 'invoked' AND retry_count > 0 THEN 'backing-off'
        ELSE 'ready'
    END, 'LargeUtf8') AS status,
    ss.completion_result,
    ss.completion_failure,
    ss.completion_failure_code
FROM restate.public.sys_invocation_state sis
RIGHT JOIN restate.public.sys_invocation_status ss
    ON ss.id = sis.id AND ss.partition_key = sis.partition_key
