SELECT
        ss.id AS id
      FROM sys_invocation_status ss
      WHERE ((ss.status = 'completed' AND ss.completion_result = 'success') OR (ss.status = 'completed' AND ss.completion_result = 'failure' AND COALESCE(LOWER(ss.completion_failure), '') NOT IN ('[409] killed', '[409] canceled', '[409] cancelled')) OR (ss.status = 'completed' AND ss.completion_result = 'failure' AND LOWER(ss.completion_failure) IN ('[409] canceled', '[409] cancelled')) OR (ss.status = 'completed' AND ss.completion_result = 'failure' AND LOWER(ss.completion_failure) IN ('[409] killed')))
        AND ss.target_service_name IN ('State')
      ORDER BY ss.created_at DESC NULLS LAST
      LIMIT 250
