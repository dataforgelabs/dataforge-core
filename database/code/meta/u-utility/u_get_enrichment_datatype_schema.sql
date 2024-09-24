
CREATE OR REPLACE FUNCTION meta.u_get_enrichment_datatype_schema(in_enr meta.enrichment)
 RETURNS json
 LANGUAGE plpgsql
AS $function$

DECLARE
    v_source_id int;
    v_metadata json;

BEGIN
    IF in_enr.rule_type_code != 'S' THEN
        RETURN in_enr.datatype_schema;
    END IF;

    SELECT s.source_id INTO v_source_id
    FROM meta.source s 
    WHERE s.sub_source_enrichment_id = in_enr.enrichment_id;

    IF v_source_id IS NULL THEN
        RETURN jsonb_build_object('error',format('u_get_enrichment_datatype_schema: Sub-source does not exist for enrichment_id=%s',in_enr.enrichment_id));
    END IF;

    IF NOT EXISTS(SELECT 1 FROM meta.enrichment e WHERE e.source_id = v_source_id AND e.active_flag) THEN
        -- sub-source has no rules, return  enrichment schema
        RETURN in_enr.datatype_schema;
    END IF;

    WITH ct AS (
        SELECT jsonb_build_object('name', r.column_alias,'type', r.datatype_schema) || 
            CASE WHEN jsonb_typeof(r.datatype_schema) = 'string' THEN jsonb_build_object('nullable', false, 'metadata', jsonb_build_object()) ELSE jsonb_build_object() END field
            , '0|' || r.column_alias sort_column
        FROM meta.raw_attribute r 
        WHERE r.source_id = v_source_id
        UNION ALL
        SELECT jsonb_build_object('name', enr.attribute_name,'type', enr.datatype_schema) || 
            CASE WHEN json_typeof(enr.datatype_schema) = 'string' THEN jsonb_build_object('nullable', false, 'metadata', jsonb_build_object()) ELSE jsonb_build_object() END
            , '1|' || enr.attribute_name sort_column
        FROM (SELECT e.attribute_name, meta.u_get_enrichment_datatype_schema(e) datatype_schema
            FROM meta.enrichment e 
            WHERE e.source_id = v_source_id AND e.active_flag ) enr
    )
    SELECT json_agg( field ORDER BY sort_column )
    INTO v_metadata
    FROM ct;

    RETURN json_build_object('type','array', 'elementType', json_build_object('type', 'struct','fields', v_metadata), 'containsNull', false);
END;

$function$;