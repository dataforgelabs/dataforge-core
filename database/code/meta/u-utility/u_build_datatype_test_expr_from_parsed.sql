CREATE OR REPLACE FUNCTION meta.u_build_datatype_test_expr_from_parsed(in_enr meta.enrichment, in_root_id text = null)
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
    v_attribute_name            text;
    v_exp_test_select           text;
    v_test_datatype_schema      jsonb;

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
        v_attribute_name := v_param.name;
        RAISE DEBUG 'v_param: %',v_param;
        IF v_param.error IS NOT NULL THEN
            RETURN v_param.error;
        END IF;

        IF v_param.datatype IS NULL THEN
            RETURN NULL;
        END IF;

        v_ret_expression := replace(v_ret_expression, format('P<%s>', v_ep.enrichment_parameter_id), 
            CASE WHEN v_aggregates_exist_flag AND v_ep.aggregation_id IS NULL 
            THEN  'first_value(' || v_attribute_name || ')' -- wrap non-aggregated parameter into aggregate for data type testing purposes only
            ELSE v_attribute_name END);

        IF in_root_id IS NOT NULL AND v_param.type IN ('raw','enrichment') THEN 
            -- substitute enrichment parameter schema with test schema passed by u_test_downstream_rules
            SELECT et.datatype_schema
            INTO v_test_datatype_schema
            FROM meta.enrichment_datatype_test et 
            WHERE ( (v_param.type = 'enrichment' AND et.enrichment_id = v_param.enrichment_id) OR (v_param.type = 'raw' AND et.raw_attribute_id = v_param.raw_attribute_id) )
            AND et.root_id = in_root_id;

            IF v_test_datatype_schema IS NOT NULL THEN
                v_param.datatype = null;
                v_param.datatype_schema = v_test_datatype_schema;
            END IF;
        END IF;

        -- add parameter with datatype
        v_exp_test_select := meta.u_datatype_test_expression(v_param.datatype,v_param.datatype_schema) || ' ' || v_attribute_name;
        IF NOT v_exp_test_select = ANY(v_exp_test_select_list) THEN
            v_exp_test_select_list := v_exp_test_select_list || v_exp_test_select;
        END IF;

    END LOOP;

    RAISE DEBUG 'v_exp_test_select_list: %',v_exp_test_select_list;
    
    IF NULLIF(in_enr.cast_datatype,'') IS NOT NULL THEN
        v_ret_expression := format('CAST(%s as %s)',v_ret_expression,in_enr.cast_datatype);
    END IF;

    IF cardinality(v_exp_test_select_list) > 0 THEN
        v_exp_test := format('WITH ct AS (SELECT %s FROM datatypes) SELECT %s as col1 FROM ct',array_to_string(v_exp_test_select_list,','), v_ret_expression);
    ELSE -- no parameters
        v_exp_test := format('SELECT %s as col1', v_ret_expression);
    END IF;

    RETURN v_exp_test;

END;

$BODY$;



CREATE OR REPLACE FUNCTION meta.u_build_datatype_test_expr_from_parsed(in_sr meta.source_relation, in_root_id text = null)
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
    v_test_datatype_schema      jsonb;

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

        IF in_root_id IS NOT NULL AND  v_param.type IN ('raw','enrichment') THEN 
            -- substitute enrichment parameter schema with test schema passed by u_test_downstream_rules
            SELECT et.datatype_schema
            INTO v_test_datatype_schema
            FROM meta.enrichment_datatype_test et 
            WHERE ( (v_param.type = 'enrichment' AND et.enrichment_id = v_param.enrichment_id) OR (v_param.type = 'raw' AND et.raw_attribute_id = v_param.raw_attribute_id) )
            AND et.root_id = in_root_id;

            IF v_test_datatype_schema IS NOT NULL THEN
                v_param.datatype = null;
                v_param.datatype_schema = v_test_datatype_schema;
            END IF;
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

