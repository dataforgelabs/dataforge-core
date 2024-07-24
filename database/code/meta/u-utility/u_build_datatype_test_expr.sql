CREATE OR REPLACE FUNCTION meta.u_build_datatype_test_expr(in_expression text, in_datatype text, in_cast_datatype text)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_end                       int;
    v_last_end                  int := 1;
    v_datatype                  text;
    v_add                       text;
    v_datatype_schema           jsonb;
    v_aggregate_id               int;
    v_aggregates_exist_flag     boolean;
    v_parameter_id               int;
    v_start                     int;
    v_exp_test_select_list      text[] := '{}';
    v_exp_test                  text;
    v_ret_expression            text    := ''; -- test expression 
    v_attribute_name            text;

BEGIN

    v_aggregates_exist_flag := EXISTS(SELECT 1 FROM _aggs_parsed);

    -- build test expression in this format:
    -- WITH ct AS (SELECT <exp1> p_1, <exp2> p_2 FROM datatypes)
    -- SELECT <expr with P<1> replaced with p_1> as col1 FROM ct

    FOR v_parameter_id, v_start, v_end, v_datatype, v_aggregate_id, v_datatype_schema, v_attribute_name
        IN SELECT id, p_start, p_end, datatype, aggregation_id, datatype_schema, attribute_name
           FROM _params ORDER BY id LOOP
        IF v_datatype IS NULL THEN
            RETURN NULL;
        END IF;

        -- add preceding characters
        RAISE DEBUG 'v_parameter_id %  start % end % last end %', v_parameter_id,  v_start, v_end, v_last_end;
        v_add := substr(in_expression,v_last_end, v_start - v_last_end);
        v_ret_expression := v_ret_expression || v_add;

        -- add parameter with datatype
        v_exp_test_select_list := v_exp_test_select_list || (meta.u_datatype_test_expression(v_datatype,v_datatype_schema) || ' ' || v_attribute_name);

        v_ret_expression := v_ret_expression || CASE WHEN v_aggregates_exist_flag AND v_aggregate_id IS NULL 
            THEN  'first_value(' || v_attribute_name || ')' -- wrap non-aggregated parameter into aggregate for data type testing purposes only
            ELSE v_attribute_name END;

        v_last_end = v_end + 1;
    END LOOP;
    -- add remaining trailing charaters
    v_ret_expression := v_ret_expression || substr(in_expression,v_last_end);

    IF NULLIF(in_cast_datatype,'') IS NOT NULL THEN
        v_ret_expression := format('CAST(%s as %s)',v_ret_expression,in_cast_datatype);
    END IF;

    RAISE DEBUG 'v_exp_test_select_list: %',v_exp_test_select_list;
    
    IF cardinality(v_exp_test_select_list) > 0 THEN
        v_exp_test := format('WITH ct AS (SELECT %s FROM datatypes) SELECT %s as col1 FROM ct',array_to_string(v_exp_test_select_list,','), v_ret_expression);
    ELSE -- no parameters
        v_exp_test := format('SELECT %s as col1', v_ret_expression);
    END IF;


    RETURN v_exp_test;

END;

$BODY$;

