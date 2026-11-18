SELECT
        v.entry_id AS id
      FROM sys_vqueues v
      WHERE v.stage = 'finished'
        AND (v.stage = 'finished' AND v.status IN ('succeeded', 'failed', 'cancelled', 'killed'))
      ORDER BY v.created_at DESC NULLS LAST
      LIMIT 250
