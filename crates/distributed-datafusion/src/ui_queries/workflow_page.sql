SELECT id
    FROM sys_invocation_status
    WHERE target_service_name = 'Promises'
      AND target_service_ty = 'workflow'
      AND target_handler_name = 'promise'
      AND target_service_key IS NOT NULL
    ORDER BY created_at DESC NULLS LAST
    LIMIT 51
