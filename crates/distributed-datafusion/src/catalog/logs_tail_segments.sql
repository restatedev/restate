CREATE VIEW restate.cluster.logs_tail_segments AS SELECT
    l.* FROM restate.cluster.logs AS l JOIN (
        SELECT log_id, max(segment_index) AS segment_index FROM restate.cluster.logs GROUP BY log_id
    ) m
    ON m.log_id = l.log_id AND l.segment_index = m.segment_index
