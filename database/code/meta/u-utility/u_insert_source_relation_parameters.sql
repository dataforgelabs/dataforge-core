CREATE OR REPLACE FUNCTION meta.u_insert_source_relation_parameters(in_field_name text, in_source_relation_id INT, in_attribute_type text,
                                                               in_id INT, in_source_id INT, in_base_source_flag boolean)
    RETURNS JSONB
    LANGUAGE plpgsql
    COST 10
AS
$function$

DECLARE
    v_source_relation_parameter_id int;
    v_self_relation_flag boolean;
    v_existing_parameter_id int;

BEGIN

    IF in_source_relation_id IS NULL THEN
        -- don't insert parameters and return dummy expression - this is check only
        RETURN jsonb_build_object('expression', CASE WHEN in_base_source_flag THEN '[This].' ELSE '[Related].' END || in_field_name, 'id', 0);
    END IF;

    SELECT sr.source_id = sr.related_source_id
    INTO v_self_relation_flag
        FROM meta.source_relation sr
    WHERE source_relation_id = in_source_relation_id;

    SELECT source_relation_parameter_id
        INTO v_existing_parameter_id
    FROM meta.source_relation_parameter
        WHERE source_relation_id = in_source_relation_id
        AND CASE
            WHEN in_attribute_type = 'raw' THEN type = 'raw' AND raw_attribute_id = in_id
            WHEN in_attribute_type = 'enrichment' THEN type = 'enrichment' AND enrichment_id = in_id
            WHEN in_attribute_type = 'system' THEN type = 'system' AND system_attribute_id = in_id
            END
        AND source_id = in_source_id
        AND self_relation_container IS NOT DISTINCT FROM CASE WHEN v_self_relation_flag THEN CASE WHEN in_base_source_flag THEN 'This' ELSE 'Related' END END;

    IF v_existing_parameter_id IS NOT NULL THEN
            RETURN jsonb_build_object('expression', CASE WHEN in_base_source_flag THEN '[This].' ELSE '[Related].' END || in_field_name, 'id', v_existing_parameter_id);

    ELSE
        -- get max Id
        SELECT COALESCE(MAX(source_relation_parameter_id),0)
        INTO v_existing_parameter_id
        FROM meta.source_relation_parameter
        WHERE source_relation_id = in_source_relation_id;

        INSERT INTO meta.source_relation_parameter (source_relation_parameter_id, source_relation_id, type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, self_relation_container) VALUES
        (v_existing_parameter_id + 1, in_source_relation_id, in_attribute_type, CASE WHEN in_attribute_type = 'enrichment' THEN in_id END, CASE WHEN in_attribute_type = 'raw' THEN in_id END,CASE WHEN in_attribute_type = 'system' THEN in_id END, in_source_id ,CASE WHEN v_self_relation_flag THEN CASE WHEN in_base_source_flag THEN 'This' ELSE 'Related' END END)
            RETURNING source_relation_parameter_id INTO v_source_relation_parameter_id;

        RETURN jsonb_build_object('expression', CASE WHEN in_base_source_flag THEN '[This].' ELSE '[Related].' END || in_field_name, 'id', v_source_relation_parameter_id);

    END IF;
END;

$function$;

CREATE OR REPLACE FUNCTION meta.u_insert_source_relation_parameters(in_parameter parameter_map, in_source_relation_id INT, in_base_source_flag boolean)
    RETURNS int
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_source_relation_parameter_id int;
    v_self_relation_flag boolean;
    v_existing_parameter_id int;

BEGIN

    SELECT sr.source_id = sr.related_source_id
    INTO v_self_relation_flag
    FROM meta.source_relation sr
    WHERE source_relation_id = in_source_relation_id;

    SELECT source_relation_parameter_id
    INTO v_existing_parameter_id
    FROM meta.source_relation_parameter
        WHERE source_relation_id = in_source_relation_id
        AND CASE
            WHEN in_parameter.type = 'raw' THEN type = 'raw' AND raw_attribute_id = in_parameter.raw_attribute_id
            WHEN in_parameter.type = 'enrichment' THEN type = 'enrichment' AND enrichment_id = in_parameter.enrichment_id
            WHEN in_parameter.type = 'system' THEN type = 'system' AND system_attribute_id = in_parameter.system_attribute_id
            END
        AND source_id = in_parameter.source_id
        AND self_relation_container IS NOT DISTINCT FROM CASE WHEN v_self_relation_flag THEN CASE WHEN in_base_source_flag THEN 'This' ELSE 'Related' END END;

    IF v_existing_parameter_id IS NOT NULL THEN
        RETURN v_existing_parameter_id;
    ELSE
        -- get max Id
        SELECT COALESCE(MAX(source_relation_parameter_id),0)
        INTO v_existing_parameter_id
        FROM meta.source_relation_parameter
        WHERE source_relation_id = in_source_relation_id;

        INSERT INTO meta.source_relation_parameter (source_relation_parameter_id, source_relation_id, type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, self_relation_container) VALUES
        (v_existing_parameter_id + 1, in_source_relation_id, in_parameter.type,  in_parameter.enrichment_id, in_parameter.raw_attribute_id, in_parameter.system_attribute_id, in_parameter.source_id ,CASE WHEN v_self_relation_flag THEN CASE WHEN in_base_source_flag THEN 'This' ELSE 'Related' END END)
            RETURNING source_relation_parameter_id INTO v_source_relation_parameter_id;

        RETURN  v_source_relation_parameter_id;

    END IF;
END;

$function$;
