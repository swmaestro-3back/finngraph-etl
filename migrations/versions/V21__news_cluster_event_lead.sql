ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS event_lead_id BIGINT;
WITH RECURSIVE up AS (
    SELECT nc.id AS start_id, nc.id, nc.parent_cluster_id, nc.link_relation, 0 AS depth
      FROM news_clusters AS nc
     WHERE nc.visible
    UNION ALL
    SELECT up.start_id, p.id, p.parent_cluster_id, p.link_relation, up.depth + 1
      FROM up
      JOIN news_clusters AS p ON p.id = up.parent_cluster_id
     WHERE up.link_relation = 'same_event'
       AND up.depth < 100
),
roots AS (
    SELECT DISTINCT ON (start_id) start_id, id AS root_id
      FROM up
     ORDER BY start_id,
              link_relation IS DISTINCT FROM 'same_event' DESC,
              CASE WHEN link_relation IS DISTINCT FROM 'same_event' THEN depth ELSE -depth END
),
leads AS (
    SELECT DISTINCT ON (r.root_id) r.root_id, nc.id AS lead_id
      FROM roots AS r
      JOIN news_clusters AS nc ON nc.id = r.start_id
     ORDER BY r.root_id,
              nc.title IS NULL OR nc.title ~ '^[[:space:]]*$',
              nc.member_count DESC,
              nc.first_published_at,
              nc.id
)
UPDATE news_clusters AS c
   SET event_lead_id = v.lead_id
  FROM (
        SELECT nc.id, l.lead_id
          FROM news_clusters AS nc
          LEFT JOIN roots AS r ON r.start_id = nc.id
          LEFT JOIN leads AS l ON l.root_id = r.root_id
       ) AS v
 WHERE c.id = v.id
   AND c.event_lead_id IS DISTINCT FROM v.lead_id;
CREATE INDEX IF NOT EXISTS news_clusters_event_lead_idx ON news_clusters (event_lead_id) WHERE event_lead_id IS NOT NULL;
