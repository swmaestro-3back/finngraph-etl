ALTER TABLE news_clusters ADD COLUMN IF NOT EXISTS visible BOOLEAN NOT NULL DEFAULT false;
UPDATE news_clusters AS c
   SET visible = v.visible
  FROM (
        SELECT nc.id,
               (nc.title IS NOT NULL AND nc.representative_news_id IS NOT NULL)
               OR EXISTS (
                    SELECT 1 FROM news AS n
                     WHERE n.cluster_id = nc.id
                       AND n.triple_extracted = true
               ) AS visible
          FROM news_clusters AS nc
       ) AS v
 WHERE c.id = v.id
   AND c.visible IS DISTINCT FROM v.visible;
CREATE INDEX IF NOT EXISTS news_clusters_visible_idx ON news_clusters (id) WHERE visible;
