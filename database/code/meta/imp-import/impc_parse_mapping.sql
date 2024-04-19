CREATE OR REPLACE FUNCTION meta.impc_parse_mapping(in_channel jsonb, in_mapping text)
    RETURNS jsonb
    LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
    v_parsed text[];
    v_aggregates text;
    v_attribute_name text;
    v_column_name text;
    v_aggregate text;
    v_keys text;
    v_ret jsonb = '{}'::jsonb;
    v_distinct_flag boolean;
    v_parameter parameter_map;
    v_output_column_id int;
BEGIN

    SELECT string_agg(aggregate_name,'|') INTO v_aggregates
    FROM meta.aggregate;

    in_mapping := trim(in_mapping);

    -- check for aggregate
    v_parsed = regexp_match(in_mapping,format('^(%s)\((distinct\s+)?(?:\[This\]\.)?(\w+)((?:\.\w+)+)?\)\s+(\w+)', v_aggregates),'i');

    IF v_parsed[1] IS NOT NULL THEN -- aggregate expression

        IF in_channel->>'operation_type' != 'Aggregate' THEN
            RETURN jsonb_build_object('error', format('Aggregation in output `%s` channel `%s` expression: %s. Please add operation_type: Aggregate to channel definition', in_channel->>'output_name', in_channel->>'output_source_name', in_mapping)); 
        END IF;

        v_aggregate := lower(v_parsed[1]);
        v_distinct_flag = v_parsed[2] ~* 'distinct\s+';	
        v_attribute_name = lower(v_parsed[3]);
        v_keys = v_parsed[4];
        v_column_name = v_parsed[5];
    ELSE -- no aggregation
        v_parsed = regexp_match(in_mapping,'^(?:\[This\]\.)?(\w+)((?:\.\w+)+)?\s+(\w+)$','i');
        v_attribute_name = lower(v_parsed[1]);
        v_keys = v_parsed[2];
        v_column_name = v_parsed[3];
    END IF;

    IF v_attribute_name IS NULL OR v_column_name IS NULL THEN
        RETURN jsonb_build_object('error', format('Invalid mapping in output `%s` channel `%s` expression: %s', in_channel->>'output_name', in_channel->>'output_source_name', in_mapping)); 
    END IF;

    v_parameter := meta.u_lookup_source_attribute((in_channel->'source_id')::int, v_attribute_name);

    IF v_parameter.error IS NOT NULL THEN
        RETURN jsonb_build_object('error', format('Invalid attribute in output `%s` channel `%s` expression: %s Details: %s', in_channel->>'output_name', in_channel->>'output_source_name', in_mapping, v_parameter.error)); 
    END IF;

    SELECT output_column_id INTO v_output_column_id
    FROM meta.output_column oc 
    WHERE oc.output_id = (in_channel->>'output_id')::int AND oc.name = v_column_name;

    IF v_output_column_id IS NULL THEN
        RETURN jsonb_build_object('error', format('Output column `%s` does not exists in output `%s` channel `%s` expression: %s output_id=%s',v_column_name, in_channel->>'output_name', in_channel->>'output_source_name', in_mapping, in_channel->>'output_id')); 
    END IF;


    RETURN to_jsonb(v_parameter) || jsonb_build_object('expression', v_attribute_name || COALESCE(v_keys,''), 'aggregate', v_aggregate, 'aggregate_distinct_flag', v_distinct_flag
     , 'keys', v_keys, 'output_column_id', v_output_column_id);

END;

$BODY$;

