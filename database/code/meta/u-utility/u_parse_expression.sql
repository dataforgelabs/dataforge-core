CREATE OR REPLACE FUNCTION meta.u_parse_expression(in_expression text, in_start int, in_end int)
  RETURNS text
    LANGUAGE 'plpgsql'

AS $BODY$
DECLARE

    v_id int;
    v_start int;
    v_end int;
--    v_datatype 
    v_aggregate_id int;
    v_last_end int := in_start;
    v_ret_expression text := '';

BEGIN

    FOR v_id, v_start, v_end   
        IN SELECT id, p_start, p_end
           FROM _params WHERE p_start >= in_start AND p_end <= in_end
           ORDER BY id LOOP
        -- add characters preceding parameter
        v_ret_expression := v_ret_expression || substr(in_expression,v_last_end,v_start - v_last_end);
        
        -- replace parameter with P<n> pointer
        v_ret_expression := v_ret_expression || 'P<' || v_id || '>';
        v_last_end := v_end + 1;
    END LOOP;

    IF v_last_end <= in_end THEN
     -- add remaining trailing charaters
        v_ret_expression := v_ret_expression || substr(in_expression,v_last_end, in_end - v_last_end  + 1);
    END IF;

    RETURN COALESCE(v_ret_expression,format('Null start=%s end=%s',in_start,in_end));

END;

$BODY$;

