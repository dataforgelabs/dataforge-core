CREATE OR REPLACE FUNCTION meta.u_read_enrichment_parameters(in_parameters json)
    RETURNS void
    LANGUAGE plpgsql
AS
$function$

BEGIN

DROP TABLE IF EXISTS _params;
-- load parameters into temp table
CREATE TEMP TABLE _params
    (id int, 
    parent_enrichment_id int, 
    type text, enrichment_id int, raw_attribute_id int, system_attribute_id int, source_id int, 
    source_relation_ids int[], 
    self_relation_container text, 
    create_datetime timestamp,
    aggregation_id int,
    source_name text,
    attribute_name text,
    paths json,
    p_start int,
    p_end int,
    datatype text,
    datatype_schema jsonb
    )  ON COMMIT DROP;

IF in_parameters IS NOT NULL THEN
    INSERT INTO _params
    SELECT * FROM json_populate_recordset(null::_params,in_parameters);
END IF;


END;
$function$;

