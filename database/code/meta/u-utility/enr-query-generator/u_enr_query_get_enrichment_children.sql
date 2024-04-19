CREATE OR REPLACE FUNCTION meta.u_enr_query_get_enrichment_children(in_source_id int, in_enr_ids int[])
 RETURNS int[] -- returns array of child enrichment_ids
 LANGUAGE plpgsql
 COST 10
AS $function$
DECLARE
v_ret int[] = '{}';
v_next int[] := in_enr_ids; 
v_child_level int;

BEGIN
RAISE DEBUG 'u_enr_query_get_enrichment_children in_enr_ids %', in_enr_ids;
FOR v_child_level IN 0 .. 1000 LOOP

-- Add directly-dependent enrichments 
    v_next :=  
        (SELECT COALESCE(array_agg(parent_enrichment_id), '{}'::int[])
            FROM meta.enrichment_parameter p
            WHERE p.type = 'enrichment' AND ARRAY[p.enrichment_id] <@ v_next
            AND p.source_id = in_source_id)
        ||

-- Add enrichments from related sources where relation expression is an enrichment
	(SELECT COALESCE(array_agg(DISTINCT ep.parent_enrichment_id), '{}'::int[])
	FROM meta.enrichment e 
	JOIN meta.enrichment_parameter ep ON e.enrichment_id = ep.parent_enrichment_id
	JOIN meta.source_relation_parameter rp ON ARRAY[rp.source_relation_id] <@ ep.source_relation_ids
	WHERE (ep.source_id <> e.source_id OR ep.self_relation_container = 'Related') AND
	e.source_id = in_source_id AND ARRAY[rp.enrichment_id] <@ v_next);

    RAISE DEBUG 'v_next=%', v_next;
    EXIT WHEN cardinality(v_next) = 0;

    v_ret := v_ret || v_next;

    PERFORM meta.u_assert( v_child_level < 1000, 'Exceeded parameter recursion limit of 1000. in_enr_ids=' || in_enr_ids::text ||
    ' source_id=' || in_source_id);
    
END LOOP;


RETURN v_ret;        
END;

$function$;