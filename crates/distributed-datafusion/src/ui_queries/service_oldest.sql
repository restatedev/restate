SELECT
        ss.id AS id
      FROM sys_invocation_status ss
      WHERE ss.target_service_name IN ('Counter')
      ORDER BY ss.created_at ASC NULLS LAST
      LIMIT 250
