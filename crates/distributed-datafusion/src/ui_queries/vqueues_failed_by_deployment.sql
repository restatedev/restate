SELECT v.entry_id AS id
FROM sys_vqueues v
WHERE v.stage = 'finished'
  AND (v.stage = 'finished'
    AND v.status IN ('failed'))
  AND v.deployment IN ('dp_14JAVb4kHpdBm6v4g3E1hM5')
LIMIT 250
