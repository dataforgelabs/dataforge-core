CREATE OR REPLACE FUNCTION meta.impc_test_expressions(in_import_id int)
    RETURNS json
    LANGUAGE plpgsql
AS
$function$

DECLARE

    v_project_id int;

BEGIN

SELECT project_id INTO v_project_id FROM meta.import WHERE import_id = in_import_id;

    RETURN (
        SELECT COALESCE(json_agg(t),'[]'::json) FROM (
            SELECT json_build_object('source_relation_id',source_relation_id, 'expression', expression) t
            FROM meta.relation_test t
            WHERE t.project_id = v_project_id AND t.expression IS NOT NULL AND result IS NULL
            UNION all
            SELECT json_build_object('enrichment_id',enrichment_id, 'expression', expression) t
            FROM meta.enrichment_test t 
            WHERE t.project_id = v_project_id AND expression IS NOT NULL AND result IS NULL
            UNION all
            SELECT json_build_object('output_source_id',output_source_id, 'expression', expression) t
            FROM meta.output_filter_test t 
            WHERE t.project_id = v_project_id AND expression IS NOT NULL AND result IS NULL
        ) t
    );
     
END;
$function$;