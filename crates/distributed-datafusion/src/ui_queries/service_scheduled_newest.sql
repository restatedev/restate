SELECT
        ss.id AS id
      FROM sys_invocation_status ss
      WHERE ss.status = 'scheduled'
        AND ss.target_service_name IN ('Counter')
      ORDER BY ss.created_at DESC NULLS LAST
      LIMIT 250
