CREATE OR REPLACE FUNCTION meta.u_enr_query_get_relation_chain_target_source_id(in_this_source_id int,
    in_relation_ids int[])
 RETURNS int -- returns target related_id
 LANGUAGE plpgsql
 
AS $function$
DECLARE
v_ret_source_id int := in_this_source_id;
v_related_source_id int;
v_source_id int;
i int;
BEGIN

PERFORM meta.u_assert(in_relation_ids IS NOT NULL, 'in_relation_ids parameter is NULL');

FOR i IN 1..array_upper(in_relation_ids,1) LOOP

	SELECT sr.source_id, sr.related_source_id
	INTO v_source_id, v_related_source_id
	FROM meta.source_relation sr
	WHERE sr.source_relation_id = in_relation_ids[i];

	PERFORM meta.u_assert( v_source_id IS NOT NULL, 'Non-existing source_relation_id=' || in_relation_ids[i] || ' detected in chain: ' || in_relation_ids::text);
	PERFORM meta.u_assert( v_ret_source_id IN (v_source_id, v_related_source_id), 'Invalid source_relation_id=' || in_relation_ids[i] || ' detected in chain: ' || in_relation_ids::text);

	v_ret_source_id := CASE WHEN v_ret_source_id = v_source_id THEN v_related_source_id ELSE v_source_id END;
END LOOP;

RETURN v_ret_source_id;
END;

$function$;