CREATE OR REPLACE FUNCTION meta.u_check_enrichment_loop(in_enrichment_id int, in_log_id int = null)
    RETURNS text
    LANGUAGE 'plpgsql'
AS $BODY$


DECLARE
    v_circular_path text[];
    v_start_node_id text = 'EN' || in_enrichment_id;

BEGIN

    IF in_enrichment_id IS NULL THEN
        RETURN null; -- skip check for new rules
    END IF;

  --Recursively get all downstream rules. 
  -- Include rules used by relations
  --Return rule chain if enrichment contains downstream self-reference


  WITH RECURSIVE ds AS (
    SELECT enrichment_id, path, 0 level
    FROM meta.u_get_upstream_rules(in_enrichment_id) ur
    UNION ALL 
    SELECT der.enrichment_id, ds.path || der.path, ds.level + 1
    FROM ds
    CROSS JOIN LATERAL meta.u_get_upstream_rules(ds.enrichment_id, ds.level + 1) der
    WHERE NOT v_start_node_id = ANY(ds.path)
  ) 
  SELECT ARRAY['EN' || in_enrichment_id] || ds.path 
  INTO v_circular_path
  FROM ds
  WHERE enrichment_id = in_enrichment_id;



 IF v_circular_path IS NOT NULL THEN
    IF in_log_id IS NOT NULL THEN -- log
        INSERT INTO log.actor_log (log_id, message, actor_path, severity, insert_datetime)
        VALUES ( in_log_id, format('Skipped attribute recalculation for self-reference enrichment loop: %s', v_circular_path),'u_check_enrichment_loop', 'W', clock_timestamp());
    END IF;
    RETURN format('Enrichment self-reference loop detected: %s', v_circular_path);
 END IF;


RETURN null;

END;
$BODY$;


