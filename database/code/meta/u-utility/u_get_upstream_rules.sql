-- return rules referenced as enrichment parameter + rules used by relations
CREATE OR REPLACE FUNCTION  meta.u_get_upstream_rules(
	in_enrichment_id int, in_level int = 0)
    RETURNS TABLE (enrichment_id int, path text[])
    LANGUAGE 'plpgsql'
AS $BODY$


BEGIN


  --Recursively get all downstream rules. 
  -- Include rules used by relations
  --Return rule chain if enrichment contains downstream self-reference

    IF in_level = 0 AND EXISTS(SELECT 1 FROM information_schema.columns where table_name = '_params' AND column_name = 'type')
        THEN
        
        RETURN QUERY (
            WITH ep AS (
                SELECT p.type, p.enrichment_id, p.source_relation_ids
                FROM _params p 
                WHERE (p.type = 'enrichment' OR p.source_relation_ids IS NOT NULL)
            )
            SELECT ep.enrichment_id, ARRAY['EN' || ep.enrichment_id]
            FROM ep 
            WHERE ep.type = 'enrichment'
            UNION ALL
            SELECT rp.enrichment_id, ARRAY['RE' || sr.source_relation_id, 'EN' || rp.enrichment_id]
            FROM meta.source_relation_parameter rp 
            JOIN meta.source_relation sr ON sr.source_relation_id = rp.source_relation_id AND sr.active_flag
            WHERE rp.type = 'enrichment' 
            AND EXISTS(SELECT 1 FROM ep WHERE rp.source_relation_id = ANY(ep.source_relation_ids))
        ); 
    
    ELSE

        RETURN QUERY (
            WITH ep AS (
                SELECT ep.type, ep.enrichment_id, ep.source_relation_ids
                FROM meta.enrichment_parameter ep
                WHERE ep.parent_enrichment_id = in_enrichment_id AND (ep.type = 'enrichment' OR ep.source_relation_ids IS NOT NULL)
            )
            SELECT ep.enrichment_id, ARRAY['EN' || ep.enrichment_id]
            FROM ep 
            WHERE ep.type = 'enrichment'
            UNION ALL
            SELECT rp.enrichment_id, ARRAY['RE' || sr.source_relation_id, 'EN' || rp.enrichment_id]
            FROM meta.source_relation_parameter rp 
            JOIN meta.source_relation sr ON sr.source_relation_id = rp.source_relation_id AND sr.active_flag
            WHERE rp.type = 'enrichment' 
            AND EXISTS(SELECT 1 FROM ep WHERE rp.source_relation_id = ANY(ep.source_relation_ids))
        ); 
    END IF;

END;
$BODY$;


