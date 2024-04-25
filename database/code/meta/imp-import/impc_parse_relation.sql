CREATE OR REPLACE FUNCTION meta.impc_parse_relation(in_parameters jsonb)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_expression text := in_parameters->>'expression';
    v_expression_parsed text := v_expression;
    v_field text;
    v_source_id int := (in_parameters->>'source_id')::int;
    v_related_source_id int := (in_parameters->>'related_source_id')::int;
    v_insert_param_id int;
    v_parameter parameter_map;
    v_this_parameter_flag boolean = false;
    v_related_parameter_flag boolean = false;


BEGIN
    IF v_source_id IS NULL THEN
        RETURN jsonb_build_object('error', 'source_id is NULL'); 
    END IF;

    IF v_related_source_id IS NULL THEN
        RETURN jsonb_build_object('error', 'v_related_source_id is NULL'); 
    END IF;

    FOR v_field IN SELECT m[1] FROM regexp_matches(in_parameters->>'expression', '(?:\[This]\.)(\w+)', 'g') m LOOP
        
        v_parameter := meta.u_lookup_source_attribute(v_source_id, v_field);

        IF v_parameter.error IS NOT NULL THEN
            RETURN jsonb_build_object('error',format('Invalid field [This].%s in relation expression. %s',v_field, v_parameter.error));
        END IF;

        v_insert_param_id := meta.u_insert_source_relation_parameters(v_parameter, (in_parameters->'source_relation_id')::int, true);
        
        v_expression := regexp_replace(v_expression,'(\[This]\.' || v_field || ')([^\w]+|$)',
            meta.u_datatype_test_expression(v_parameter.datatype, v_parameter.datatype_schema) || '\2','g');

        v_expression_parsed := regexp_replace(v_expression_parsed,'(\[This]\.' || v_field || ')([^\w]+|$)',
            'P<' || v_insert_param_id || '>\2','g');
        
        v_this_parameter_flag := true;
    END LOOP;

    IF NOT v_this_parameter_flag THEN
        RETURN jsonb_build_object('error',format('Missing [This] attribute in relation expression. %s',in_parameters->>'expression'));
    END IF;

    FOR v_field IN SELECT m[1] FROM regexp_matches(in_parameters->>'expression', '(?:\[Related]\.)(\w+)', 'g') m LOOP
        
        v_parameter := meta.u_lookup_source_attribute(v_related_source_id, v_field);

        IF v_parameter.error IS NOT NULL THEN
            RETURN jsonb_build_object('error',format('Invalid field [Related].%s in relation expression. %s',v_field, v_parameter.error));
        END IF;

        v_insert_param_id := meta.u_insert_source_relation_parameters(v_parameter, (in_parameters->'source_relation_id')::int, false);
        
        v_expression := regexp_replace(v_expression,'(\[Related]\.' || v_field || ')([^\w]+|$)',
            meta.u_datatype_test_expression(v_parameter.datatype, v_parameter.datatype_schema) || '\2','g');

        v_expression_parsed := regexp_replace(v_expression_parsed,'(\[Related]\.' || v_field || ')([^\w]+|$)',
            'P<' || v_insert_param_id || '>\2','g');
        
        v_related_parameter_flag := true;
    END LOOP;

    IF NOT v_related_parameter_flag THEN
        RETURN jsonb_build_object('error',format('Missing [Related] attribute. expression. %s',v_expression));
    END IF;


    RETURN json_build_object('test_expression', 'SELECT ' || v_expression || ' as col1 FROM datatypes'
    , 'expression_parsed', v_expression_parsed);

END;

$BODY$;

