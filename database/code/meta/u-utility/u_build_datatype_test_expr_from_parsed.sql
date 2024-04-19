CREATE OR REPLACE FUNCTION meta.u_build_datatype_test_expr_from_parsed(in_enr meta.enrichment)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_param                     parameter_map;
    v_ep                        meta.enrichment_parameter;
    v_aggregates_exist_flag     boolean = false;
    v_exp_test_select_list      text[] := '{}';
    v_exp_test                  text;
    v_ret_expression            text :=  in_enr.expression_parsed;
    v_agg                       text;
    v_id                        int;

BEGIN

    IF v_ret_expression IS NULL THEN
        RETURN NULL;
    END IF;

    -- replace aggregates
    FOR v_id, v_agg IN SELECT enrichment_aggregation_id, expression FROM meta.enrichment_aggregation WHERE enrichment_id = in_enr.enrichment_id LOOP
        v_ret_expression := replace(v_ret_expression, format('A<%s>', v_id), v_agg);
        v_aggregates_exist_flag = true;
    END LOOP;

    RAISE DEBUG 'v_ret_expression: %',v_ret_expression;
    -- build test expression in this format:
    -- WITH ct AS (SELECT <exp1> p_1, <exp2> p_2 FROM datatypes)
    -- SELECT <expr with P<1> replaced with p_1> as col1 FROM ct

    FOR v_ep IN SELECT *
           FROM meta.enrichment_parameter ep WHERE parent_enrichment_id = in_enr.enrichment_id ORDER BY enrichment_parameter_id LOOP
        
        v_param := meta.u_get_parameter(v_ep);
        RAISE DEBUG 'v_param: %',v_param;
        IF v_param.error IS NOT NULL THEN
            RETURN v_param.error;
        END IF;

        IF v_param.datatype IS NULL THEN
            RETURN NULL;
        END IF;

        v_ret_expression := replace(v_ret_expression, format('P<%s>', v_ep.enrichment_parameter_id), format('p_%s', v_ep.enrichment_parameter_id)) ;

        -- add parameter with datatype
        v_exp_test_select_list := v_exp_test_select_list ||
         (CASE WHEN v_aggregates_exist_flag AND v_ep.aggregation_id IS NULL 
            THEN  'first_value(' || meta.u_datatype_test_expression(v_param.datatype,v_param.datatype_schema) || ')' -- wrap non-aggregated parameter into aggregate for data type testing purposes only
            ELSE  meta.u_datatype_test_expression(v_param.datatype,v_param.datatype_schema) END  || ' p_' || v_ep.enrichment_parameter_id);

    END LOOP;

    RAISE DEBUG 'v_exp_test_select_list: %',v_exp_test_select_list;
    
    IF cardinality(v_exp_test_select_list) > 0 THEN
        v_exp_test := format('WITH ct AS (SELECT %s FROM datatypes) SELECT %s as col1 FROM ct',array_to_string(v_exp_test_select_list,','), v_ret_expression);
    ELSE -- no parameters
        v_exp_test := format('SELECT %s as col1', v_ret_expression);
    END IF;

    RETURN v_exp_test;

END;

$BODY$;



CREATE OR REPLACE FUNCTION meta.u_build_datatype_test_expr_from_parsed(in_sr meta.source_relation)
    RETURNS text
    LANGUAGE 'plpgsql'

AS
$BODY$

DECLARE
    v_param                     parameter_map;
    v_ep                        meta.source_relation_parameter;
    v_exp_test_select_list      text[] := '{}';
    v_exp_test                  text;
    v_ret_expression            text :=  in_sr.expression_parsed;

BEGIN

    IF v_ret_expression IS NULL THEN
        RETURN NULL;
    END IF;

    -- build test expression in this format:
    -- WITH ct AS (SELECT <exp1> p_1, <exp2> p_2 FROM datatypes)
    -- SELECT <expr with P<1> replaced with p_1> as col1 FROM ct

    FOR v_ep IN SELECT *
           FROM meta.source_relation_parameter rp WHERE source_relation_id = in_sr.source_relation_id ORDER BY source_relation_parameter_id LOOP
        
        v_param := meta.u_get_parameter(v_ep);
        RAISE DEBUG 'v_param: %',v_param;
        IF v_param.error IS NOT NULL THEN
            RETURN v_param.error;
        END IF;

        IF v_param.datatype IS NULL THEN
            RETURN NULL;
        END IF;

        v_ret_expression := replace(v_ret_expression, format('P<%s>', v_ep.source_relation_parameter_id), format('p_%s', v_ep.source_relation_parameter_id)) ;

        -- add parameter with datatype
        v_exp_test_select_list := v_exp_test_select_list ||
          ( meta.u_datatype_test_expression(v_param.datatype,v_param.datatype_schema) || ' p_' || v_ep.source_relation_parameter_id);

    END LOOP;

    RAISE DEBUG 'v_exp_test_select_list: %',v_exp_test_select_list;
    
    IF cardinality(v_exp_test_select_list) > 0 THEN
        v_exp_test := format('WITH ct AS (SELECT %s FROM datatypes) SELECT %s as col1 FROM ct',array_to_string(v_exp_test_select_list,','), v_ret_expression);
    ELSE -- no parameters
        v_exp_test := format('SELECT %s as col1', v_ret_expression);
    END IF;

    RETURN v_exp_test;

END;

$BODY$;

