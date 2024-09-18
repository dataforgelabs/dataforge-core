CREATE OR REPLACE FUNCTION meta.svc_parse_enrichment(in_parameters JSON, in_template_check_flag boolean DEFAULT FALSE, in_mode text = 'ui')
    RETURNS JSON
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    in_enr                       meta.enrichment;
    v_ret_expression             TEXT    := ''; -- expression with  attributes replaced with datatypes
    v_expression_parsed          TEXT    := ''; -- parsed expression
    v_attribute_name             TEXT;
    v_source_name                TEXT;
    v_parameter_source_id        INT;
    v_attribute_name_error       TEXT;
    v_enrichment_name_error      Text;
    v_attribute_check_json       JSON;
    v_expression_position        INT     := 0;
    v_parameter_position         INT     := 0;
    v_in_square_brackets_flag    BOOLEAN := FALSE;
    v_in_quotes_flag             BOOLEAN := FALSE;
    v_expression_length          INT;
    v_in_attribute_flag          BOOLEAN;
    v_field_start_position       INT;
    v_char                       CHAR;
    v_next_char                  CHAR;
    v_window_function_flag       BOOLEAN;
    v_attribute_start_position   INT;
    v_template_match_flag        BOOLEAN := FALSE;
    v_ret_params                 json;
    v_ret_aggs                   json;
    v_ret_enr                    jsonb; 
    v_agg_error                  text;
    v_aggregate_id               int;
    v_project_id                 int;
    v_parameter                  parameter_map;
    v_saved_parameter_position   int;
    v_next_relation_path         json;
    v_enrichment_parameter_id    int;
    v_source_relation_ids        int[];
    v_start                     int;
    v_agg_start                 int;
    v_end                       int;
    v_last_end                  int := 1;
    v_error_ids                 int[];
    v_relation_path_count       int;
    v_processing_type           text;

BEGIN

SELECT * FROM json_populate_record(null::meta.enrichment, in_parameters -> 'enrichment' )  INTO in_enr;

IF in_enr.expression IS NULL THEN
    RETURN  json_build_object('error', 'Expression is NULL', 'expression', NULL);
END IF;

v_project_id := meta.u_get_source_project(in_enr.source_id);

IF (in_enr.expression) LIKE '%/*%'
THEN
    RETURN json_build_object('error', 'Please use the description section for any comments.', 'expression', NULL);
END IF;

-- check attribute name syntax
IF NOT in_enr.attribute_name ~ '^[a-z_]+[a-z0-9_]*$'
THEN
    v_attribute_name_error := 'Invalid attribute name syntax. Attribute name has to start with lowercase letter or _ It may contain lowercase letters, numbers and _';
END IF;

v_template_match_flag := in_template_check_flag and exists (select 1 from meta.enrichment WHERE source_id = in_enr.source_id AND expression = in_enr.expression AND attribute_name = in_enr.attribute_name AND name = in_enr.name);

-- check attribute name uniquiness
v_attribute_check_json := meta.svc_check_attribute_name(in_enr.source_id, in_enr.attribute_name);
IF v_attribute_check_json ->> 'attribute_type' IN ('raw', 'system') OR (
        v_attribute_check_json ->> 'attribute_type' = 'enrichment' AND (v_attribute_check_json ->> 'id')::int IS DISTINCT FROM in_enr.enrichment_id AND NOT v_template_match_flag)
THEN
    v_attribute_name_error := 'Invalid attribute name: ' || (v_attribute_check_json ->> 'attribute_type')
        || ' attribute with this name already exists. ' || CASE WHEN in_template_check_flag THEN 'Name, Attribute Name, Source_id, and Expression must match EXACTLY to apply a template to an existing rule.' ELSE '' END;
END IF;
-- check enrichment name uniqueness
v_enrichment_name_error := CASE WHEN EXISTS(SELECT 1
                                    FROM meta.enrichment e
                                    WHERE e.name = in_enr.name AND e.source_id = in_enr.source_id AND e.active_flag AND
                                        e.enrichment_id IS DISTINCT FROM in_enr.enrichment_id AND NOT v_template_match_flag) THEN 'Duplicate enrichment name. ' || CASE WHEN in_template_check_flag THEN 'Name, Attribute Name, Source_id, and Expression must match EXACTLY to apply a template to an existing rule.' ELSE '' END 
                                        END;

IF v_enrichment_name_error IS NOT NULL OR v_attribute_name_error IS NOT NULL
THEN
    RETURN json_build_object('error', COALESCE(v_attribute_name_error,'') || COALESCE(v_enrichment_name_error,''));
END IF;

in_enr.window_function_flag := in_enr.expression ~* 'over\s*\(.*\)';

IF NOT in_enr.keep_current_flag AND in_enr.window_function_flag THEN
    RETURN json_build_object('error', 'When expression contains window function, enrichment is required to use keep current recalculation mode');
END IF;


SELECT processing_type INTO v_processing_type
FROM meta.source
WHERE source_id = in_enr.source_id;

IF v_processing_type = 'stream' THEN
    IF in_enr.keep_current_flag THEN
        RETURN json_build_object('error', 'Keep current recalculation mode is not supported on stream sources');
    END IF;
    IF in_enr.unique_flag THEN
        RETURN json_build_object('error', 'Unique rules are not supported on stream sources');
    END IF;
END IF;

PERFORM meta.u_read_enrichment_parameters(in_parameters -> 'params');

IF NOT EXISTS(SELECT 1 FROM _params) AND in_enr.enrichment_id IS NOT NULL THEN
    -- enrichment is saved, but no parameters passed: read from enrichment_parameters
    INSERT INTO _params
    SELECT ep.enrichment_parameter_id, 
    ep.parent_enrichment_id,
    ep.type,
    ep.enrichment_id,
    ep.raw_attribute_id,
    ep.system_attribute_id,
    ep.source_id,
    ep.source_relation_ids,
    ep.self_relation_container,
    ep.create_datetime,
    ep.aggregation_id,
    CASE WHEN ep.source_id <> in_enr.source_id THEN s.source_name -- Use 'This' when parameter is from current source and is not using relation
    WHEN COALESCE(ep.source_relation_ids,'{}'::int[]) = '{}' THEN 'This' ELSE s.source_name END,  
    meta.u_enr_query_get_enrichment_parameter_name(ep) attribute_name,
    CASE WHEN  COALESCE(ep.source_relation_ids,'{}'::int[]) <> '{}'::int[] THEN meta.u_get_next_relation_path(in_enr.source_id, ep.source_id, CASE WHEN ep.aggregation_id IS NULL THEN '1' ELSE 'M' END, ep.source_relation_ids) END paths,
    null::int p_start, null::int p_end, null, null
    FROM meta.enrichment_parameter ep
    LEFT JOIN meta.source s ON ep.source_id = s.source_id
    WHERE parent_enrichment_id = in_enr.enrichment_id;
END IF;


-- parse aggregates
v_agg_error := meta.u_parse_enrichment_aggregates(in_enr.expression);
IF v_agg_error IS DISTINCT FROM '' THEN
    RETURN json_build_object('error', v_agg_error);
END IF;

RAISE DEBUG 'Parsed % aggregates',(SELECT COUNT(1) FROM  _aggs_parsed);

    v_expression_length = length(in_enr.expression);

    WHILE v_expression_position <= v_expression_length LOOP
        v_expression_position := v_expression_position + 1;
        v_char = substring(in_enr.expression, v_expression_position, 1);

        IF (v_in_quotes_flag)
        THEN
            IF v_char = ''''
            THEN
                v_next_char := substring(in_enr.expression, v_expression_position + 1, 1);
                IF v_next_char = ''''
                THEN
                    --Double quote escape character. Keep going
                    v_expression_position := v_expression_position + 1;
                ELSE
                    v_in_quotes_flag := FALSE;
                END IF;
            ELSE
                --Still in a quoted string, keep going down the string
            END IF;

        ELSEIF v_in_square_brackets_flag
            THEN
                --Check for error
                IF v_char = '['
                THEN
                    RETURN json_build_object('error', 'Nested [ brackets detected. Found ' || v_char || ' at position ' || v_expression_position || '.');
                --Check to see square bracket is ending
                ELSEIF v_char = ']'
                THEN
                    -- check empty brackets
                    IF v_expression_position - v_field_start_position = 1 THEN 
                        RETURN json_build_object('error', 'Empty brackets at position ' || v_expression_position);
                    END IF;

                    v_source_name = substring(in_enr.expression, v_field_start_position + 1, v_expression_position - v_field_start_position - 1);

                    IF v_source_name ~ '^[0-9]+$' THEN -- numbers only, ignore as array index
                        v_in_square_brackets_flag := false;
                        CONTINUE;
                    END IF;    

                    --Check next char is valid
                    v_next_char = substring(in_enr.expression, v_expression_position + 1, 1);
                    IF v_next_char <> '.' THEN
                        RETURN json_build_object('error', 'Improper expression detected. Found ' || v_char || ' at position ' || v_expression_position || '. Expected .');
                    ELSE
                        --If its kosher, we are out of the square brackets. Check source name

                    v_parameter_source_id := null;
                    IF v_source_name = 'This' THEN
                        v_parameter_source_id = in_enr.source_id;
                    ELSE
                        SELECT s.source_id INTO v_parameter_source_id
                        FROM meta.source s
                        WHERE s.project_id = v_project_id AND s.source_name = v_source_name;
                    END IF;

                    IF v_parameter_source_id IS NULL THEN
                        RETURN json_build_object('error', 'Source name ' || v_source_name || ' does not exist in project_id=' || v_project_id || ' position ' || v_expression_position);
                    END IF;
                        -- source validated, move on to attribute
                        v_in_square_brackets_flag := FALSE;
                        v_expression_position := v_expression_position + 2;
                        v_attribute_start_position := v_expression_position;
                        v_in_attribute_flag = true;
                    END IF;
                ELSE
                    --We aren't out of the square brackets yet. Keep going.
                END IF;
        ELSEIF v_in_attribute_flag
            THEN
                IF NOT v_char ~ '\w' OR v_expression_position > v_expression_length 
                THEN
                    --End of field, send if off
                    v_in_attribute_flag = FALSE;

                    v_attribute_name := substring(in_enr.expression, v_attribute_start_position, v_expression_position - v_attribute_start_position);
                   
                    -- check self-reference
                    IF v_attribute_name = in_enr.attribute_name AND v_parameter_source_id = in_enr.source_id THEN
                        RETURN json_build_object('error', 'Self-referencing attribute detected in postition ' || v_attribute_start_position);
                    END IF;
                    -- lookup parameter attribute
                    v_parameter := meta.u_lookup_source_attribute(v_parameter_source_id, v_attribute_name);
                    IF v_parameter.error IS NOT NULL THEN
                        RETURN json_build_object('error', v_parameter.error);
                    END IF;

                    IF v_parameter.enrichment_id = in_enr.enrichment_id THEN
                        RETURN json_build_object('error', format('Self-reference detected in attribute [%s].%s at position %s',v_source_name, v_attribute_name, v_attribute_start_position));
                    END IF;

                    SELECT id INTO v_aggregate_id
                    FROM _aggs_parsed
                    WHERE v_field_start_position BETWEEN a_start AND a_end 
                        AND v_expression_position - 1 BETWEEN a_start AND a_end;

                    IF v_source_name = 'This' AND v_aggregate_id IS NOT NULL THEN
                        RETURN json_build_object('error', format('Aggregate not allowed on [This] source attribute %s at position %s', v_attribute_name, v_attribute_start_position));
                    END IF;

                    IF v_processing_type = 'stream' AND v_aggregate_id IS NOT NULL THEN
                        RETURN json_build_object('error', format('Aggregates are not allowed on stream source rules. attribute %s at position %s', v_attribute_name, v_attribute_start_position));
                    END IF;

                    v_parameter_position := v_parameter_position + 1;

                    -- we have parameter at v_parameter_position, let's compare it with what is currently saved 
                    SELECT id, 
                    CASE WHEN meta.u_compare_nulls(aggregation_id, v_aggregate_id) THEN source_relation_ids ELSE '{}'::int[] END -- reset saved path when was aggregation  
                    INTO v_enrichment_parameter_id, v_source_relation_ids 
                    FROM _params p
                    WHERE p.source_name = v_source_name AND p.attribute_name = v_attribute_name AND p.id = v_parameter_position;

                    IF v_enrichment_parameter_id IS NOT NULL THEN
                        -- saved or passed parameter exists in the same position, update aggregation_id & path if changed,continue
                        RAISE DEBUG 'v_attribute_name % exists in same position',v_attribute_name;
                        IF v_source_name <> 'This' THEN-- update paths and positions
                            v_next_relation_path := meta.u_get_next_relation_path(in_enr.source_id, v_parameter_source_id, CASE WHEN v_aggregate_id IS NULL THEN '1' ELSE 'M' END,v_source_relation_ids);
                            UPDATE _params SET paths = v_next_relation_path, source_relation_ids = meta.u_json_array_to_int_array(v_next_relation_path->'relation_ids'),
                            p_start = v_field_start_position, p_end = v_expression_position - 1,
                            aggregation_id = v_aggregate_id,
                            datatype = v_parameter.datatype,
                            datatype_schema = v_parameter.datatype_schema
                            WHERE id = v_enrichment_parameter_id;
                        ELSE -- update positions
                            UPDATE _params SET 
                            p_start = v_field_start_position, p_end = v_expression_position - 1,
                            datatype = v_parameter.datatype,
                            datatype_schema = v_parameter.datatype_schema
                            WHERE id = v_enrichment_parameter_id;
                        END IF;
                        CONTINUE;
                    END IF;

                    
                    -- search parameter with same name and aggregation
                    IF NOT EXISTS(SELECT 1 FROM _params p
                        WHERE p.source_name = v_source_name AND p.attribute_name = v_attribute_name AND meta.u_compare_nulls(aggregation_id, v_aggregate_id)
                        ) THEN
                        -- parameter doesn't exist: insert it and slide positions of all following parameters
                        RAISE DEBUG 'v_attribute_name % doesn''t exists, inserting into position %',v_attribute_name,v_parameter_position;
                        UPDATE _params SET id = id + 1
                        WHERE  id >= v_parameter_position;

                        v_source_relation_ids := meta.u_get_relation_path_core(in_enr.enrichment_id, v_source_name);

                        -- get relation paths starting from blank chain
                        v_next_relation_path := CASE WHEN v_source_name <> 'This' 
                            THEN meta.u_get_next_relation_path(in_enr.source_id, v_parameter_source_id, 
                            CASE WHEN v_aggregate_id IS NULL THEN '1' ELSE 'M' END,
                            v_source_relation_ids -- get path from core import parameter temp table if it exists, otherwise null
                            ) 
                        END;

                        IF  in_mode = 'import' AND v_source_relation_ids IS NULL THEN
                                                -- check for multiple paths

                            IF EXISTS(SELECT 1 FROM json_array_elements(v_next_relation_path->'path') h
                                        WHERE json_array_length(h->'selections') > 1)
                                THEN 
                                    RETURN json_build_object('error', format('Multiple relation paths exist for source %s. Specify desired path in rule parameters.',v_source_name),
                                    'details', v_next_relation_path);
                            END IF;			
                        END IF;			

                        INSERT INTO _params (id, parent_enrichment_id, 
                            type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, 
                            source_relation_ids, 
                            self_relation_container, 
                            aggregation_id,
                            source_name,
                            attribute_name ,
                            paths ,
                            p_start ,
                            p_end,
                            datatype,
                            datatype_schema 
                            )
                        VALUES (
                            v_parameter_position, in_enr.enrichment_id,
                            v_parameter.type, v_parameter.enrichment_id, v_parameter.raw_attribute_id , v_parameter.system_attribute_id ,v_parameter_source_id,
                            meta.u_json_array_to_int_array(v_next_relation_path->'relation_ids'),
                            CASE WHEN v_source_name <> 'This' AND in_enr.source_id = v_parameter_source_id THEN 'Related' END, -- TODO: this can be simplified into a flag
                            v_aggregate_id,
                            v_source_name,
                            v_attribute_name,
                            v_next_relation_path,
                            v_field_start_position,
                            v_expression_position - 1,
                            v_parameter.datatype,
                            v_parameter.datatype_schema
                        );
                        CONTINUE;
                    END IF;

                    -- search parameter later in expression with the same aggregation
                    SELECT MIN(id) INTO v_saved_parameter_position
                    FROM _params p
                    WHERE p.source_name = v_source_name AND p.attribute_name = v_attribute_name 
                    AND meta.u_compare_nulls(aggregation_id, v_aggregate_id)
                    AND id > v_parameter_position;
                    
                    IF v_saved_parameter_position IS NOT NULL THEN
                        -- parameter exists in later/greater position. This means that saved/passed parameter in current position was either removed from expression or order of parameters in the expression has changed 
                        -- swap positions of saved and current parameters

                        RAISE DEBUG 'v_attribute_name % exists later at position %',v_attribute_name,v_saved_parameter_position;
                        UPDATE _params SET id = -1
                        WHERE  id = v_parameter_position;
                        
                        UPDATE _params SET id = v_parameter_position,
                            p_start = v_field_start_position,
                            p_end = v_expression_position - 1,
                            aggregation_id = v_aggregate_id,
                            datatype = v_parameter.datatype,
                            datatype_schema = v_parameter.datatype_schema
                        WHERE  id = v_saved_parameter_position;

                        UPDATE _params SET id = v_saved_parameter_position
                        WHERE  id = -1;


                        CONTINUE;
                    END IF;
                    
                    SELECT MAX(id) INTO v_saved_parameter_position
                    FROM _params p
                    WHERE p.source_name = v_source_name AND p.attribute_name = v_attribute_name AND meta.u_compare_nulls(aggregation_id, v_aggregate_id);


                    IF v_saved_parameter_position IS NOT NULL THEN
                        -- parameter exists in preceding position (duplicate): copy it to current position, including path (if aggregations didn't change)
                        RAISE DEBUG 'v_attribute_name % exists earlier at position %',v_attribute_name,v_saved_parameter_position;
                        -- push parameters down
                        UPDATE _params SET id = id + 1
                        WHERE  id >= v_parameter_position;
                        
                        INSERT INTO _params (id, parent_enrichment_id, 
                            type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, 
                            source_relation_ids, 
                            self_relation_container, 
                            aggregation_id,
                            source_name,
                            attribute_name,
                            paths,
                            p_start,
                            p_end, 
                            datatype,
                            datatype_schema
                            )
                        SELECT v_parameter_position, parent_enrichment_id,
                            type, enrichment_id, raw_attribute_id, system_attribute_id, source_id, 
                            source_relation_ids, 
                            self_relation_container, 
                            v_aggregate_id,
                            source_name,
                            attribute_name,
                            paths,
                            v_field_start_position,
                            v_expression_position - 1,
                            v_parameter.datatype,
                            v_parameter.datatype_schema
                        FROM _params
                        WHERE id = v_saved_parameter_position;
                    ELSE
                        -- this should never happen because we checked all combinations above
                        RETURN json_build_object('error', 'Unable to parse parameter [' || COALESCE(v_source_name,'null') || '].' || COALESCE(v_attribute_name,'null') || ' at position ' || v_field_start_position);
                    END IF;


                ELSE
                    --Still in field keep trucking
                END IF;
        ELSEIF v_char = '['  THEN
                v_in_square_brackets_flag = TRUE;
                v_field_start_position = v_expression_position;
        ELSEIF v_char = '''' THEN
                v_in_quotes_flag = TRUE;
        END IF;

        
    END LOOP;
    -- delete passed/saved parameters after last parsed parameter position
    DELETE FROM _params WHERE id > v_parameter_position;


    -- create test expression
    v_ret_expression := meta.u_build_datatype_test_expr(in_enr.expression, in_enr.datatype, in_enr.cast_datatype);
    
    SELECT json_agg(p) INTO v_ret_params FROM _params p;

    IF v_ret_expression IS NULL AND in_mode = 'ui' THEN
        SELECT json_agg(a) INTO v_ret_aggs FROM _aggs_parsed a;
        RETURN json_build_object('error', format('Unable to parse expression for data type checking. params: %s aggs: %s', v_ret_params, v_ret_aggs));
    END IF;
    -- create parsed expression
    -- replace all aggregate functions in original expression with A<N> pointers
    -- loop through each aggregate and replace parameter expressions with P<N> pointers
    v_last_end = 1;
    FOR v_aggregate_id, v_agg_start, v_start, v_end IN SELECT id, a_function_start, a_start, a_end FROM _aggs_parsed ORDER BY id LOOP
            -- add chunk after last aggregate
            v_expression_parsed := v_expression_parsed || meta.u_parse_expression(in_enr.expression,v_last_end,v_agg_start - 1);
                -- add A<N> pointer
            v_expression_parsed := v_expression_parsed || 'A<' || v_aggregate_id || '>';
            -- check that only single related parameter is used by an aggregation
            SELECT array_agg(id), count(DISTINCT source_relation_ids), max(source_relation_ids)
            INTO v_error_ids, v_relation_path_count, v_source_relation_ids
            FROM _params WHERE aggregation_id = v_aggregate_id AND source_relation_ids IS NOT NULL;

            IF v_relation_path_count > 1 THEN
                RETURN json_build_object('error', format('Multiple parameters %s found in agggregation %s using different relation paths',v_error_ids, v_aggregate_id),
                'params', v_ret_params);
            END IF;

            UPDATE _aggs_parsed a SET expression_parsed = a.function || '(' || meta.u_parse_expression(in_enr.expression,v_start,v_end - 1) || ')',
            relation_ids = v_source_relation_ids
            WHERE id = v_aggregate_id;
        v_last_end = v_end + 1;
    END LOOP;
    -- add expression after last aggregate
    v_expression_parsed := v_expression_parsed || meta.u_parse_expression(in_enr.expression,v_last_end,length(in_enr.expression));
    in_enr.expression_parsed := v_expression_parsed;

    SELECT json_agg(a) INTO v_ret_aggs FROM _aggs_parsed a;

    v_ret_enr := to_jsonb(in_enr);

    IF in_enr.rule_type_code = 'S' THEN
        v_ret_enr := v_ret_enr || jsonb_build_object('sub_source_id', 
                (SELECT source_id FROM meta.source s WHERE s.sub_source_enrichment_id = in_enr.enrichment_id)); 
    END IF;

RETURN json_strip_nulls(json_build_object('expression', v_ret_expression, 'enrichment', v_ret_enr, 'params', v_ret_params, 'aggs', v_ret_aggs));

END;

$BODY$;