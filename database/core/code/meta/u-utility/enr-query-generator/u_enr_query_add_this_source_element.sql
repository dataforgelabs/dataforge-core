CREATE OR REPLACE FUNCTION meta.u_enr_query_add_this_source_element(v_parameter meta.enrichment_parameter)
RETURNS int
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
    v_ret int;
    BEGIN
 -- [This] source:  enrichment_parameter.source_id = enrichment.source_id
            IF v_parameter.type = 'enrichment' THEN
                -- Enrichment parameter from same source: add parent enrichment element
                SELECT meta.u_enr_query_add_enrichment(enr)
                INTO v_ret
                FROM meta.enrichment enr
                WHERE enr.enrichment_id = v_parameter.enrichment_id;

                ELSEIF v_parameter.type = 'raw' THEN
                -- Raw parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'raw' AND e.attribute_id = v_parameter.raw_attribute_id;

            ELSEIF v_parameter.type = 'system' THEN
                -- System parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'system' AND e.attribute_id = v_parameter.system_attribute_id;

                END IF;
    PERFORM meta.u_assert( v_ret IS NOT NULL, 'unable to lookup [This] source enrichment parameter: ' || to_json(v_parameter)::text);


 RETURN v_ret;

END;
$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_this_source_element(v_parameter meta.source_relation_parameter)
RETURNS int
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
    v_ret int;
    BEGIN
 -- [This] source:  enrichment_parameter.source_id = enrichment.source_id
            IF v_parameter.type = 'enrichment' THEN
                -- Enrichment parameter from same source: add parent enrichment element
                SELECT meta.u_enr_query_add_enrichment(enr)
                INTO v_ret
                FROM meta.enrichment enr
                WHERE enr.enrichment_id = v_parameter.enrichment_id;

                ELSEIF v_parameter.type = 'raw' THEN
                -- Raw parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'raw' AND e.attribute_id = v_parameter.raw_attribute_id;

            ELSEIF v_parameter.type = 'system' THEN
                -- System parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'system' AND e.attribute_id = v_parameter.system_attribute_id;

                END IF;

    PERFORM meta.u_assert( v_ret IS NOT NULL, 'unable to lookup [This] source relation parameter %s' || to_json(v_parameter)::text);

 RETURN v_ret;

END;
$function$;

CREATE OR REPLACE FUNCTION meta.u_enr_query_add_this_source_element(in_type text, in_id int)
RETURNS int
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_ret int;
    BEGIN
 -- [This] source:  enrichment_parameter.source_id = enrichment.source_id
            IF in_type = 'enrichment' THEN
                -- Enrichment parameter from same source: add parent enrichment element
                SELECT meta.u_enr_query_add_enrichment(enr)
                INTO v_ret
                FROM meta.enrichment enr
                WHERE enr.enrichment_id = in_id;

            ELSEIF in_type = 'raw' THEN
                -- Raw parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'raw' AND e.attribute_id = in_id;

            ELSEIF in_type = 'system' THEN
                -- System parameter from same source: add parent elements
                SELECT e.id
                INTO v_ret
                FROM elements e
                WHERE e.type = 'system' AND e.attribute_id = in_id;

                END IF;

 RETURN v_ret;

END;
$function$;