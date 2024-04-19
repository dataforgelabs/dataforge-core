CREATE OR REPLACE FUNCTION meta.svc_check_attribute_name(in_source_id int, in_name text)
    RETURNS json
    LANGUAGE 'plpgsql'

    COST 10
    VOLATILE 
AS $BODY$

DECLARE 
v_ret json;
BEGIN

 		WITH ct AS(
		SELECT 'raw' as attribute_type, raw_attribute_id as id
        FROM meta.raw_attribute r
        WHERE r.source_id = in_source_id AND r.column_alias = in_name
        UNION ALL
        SELECT 'system', system_attribute_id
        FROM meta.system_attribute s
        WHERE s.table_type @> ARRAY['hub'] AND s.name = in_name
        UNION ALL
        SELECT 'enrichment', enrichment_id
        FROM meta.enrichment e 
        WHERE e.source_id = in_source_id AND e.active_flag AND e.attribute_name = in_name
		 )
		SELECT row_to_json(ct)
		 INTO v_ret
		 FROM ct; 

	RETURN  v_ret;
		
END;

$BODY$;