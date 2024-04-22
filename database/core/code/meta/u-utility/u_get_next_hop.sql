
CREATE OR REPLACE FUNCTION meta.u_get_next_hop(in_relation_ids int[], in_level int, in_start_path int[])
 RETURNS TABLE (next_relation_id int, next_complete_flag boolean, next_hop_exists_flag boolean, reverse_flag boolean)
 LANGUAGE plpgsql
AS $function$

BEGIN

	DROP TABLE IF EXISTS _next_hop;
	CREATE TEMP TABLE _next_hop ON COMMIT DROP AS
		SELECT relation_ids[in_level] relation_id, -- next relation in path
		bool_or(cardinality(relation_ids) = in_level) as complete_flag, -- TRUE when this relation forms complate path which ends at the destination source
		bool_or(sr.primary_flag) primary_flag, 
		count(1) > 1 as next_hop_exists_flag, -- TRUE when more relations can be added to existing path
		min(path_level) min_length,
		reverse_flags[in_level] reverse_flag
		FROM _paths JOIN meta.source_relation sr ON sr.source_relation_id = relation_ids[in_level]
		WHERE meta.u_array_starts_with(relation_ids,in_relation_ids) -- keep only paths we found on prior iterations
		GROUP BY relation_ids[in_level],reverse_flags[in_level];

	RETURN QUERY (
		SELECT n.relation_id, n.complete_flag, n.next_hop_exists_flag, n.reverse_flag
		FROM _next_hop n
		WHERE (cardinality(in_start_path) < in_level OR in_start_path[in_level] = relation_id ) 
			--AND (v_level = 1 OR v_next_relation_id IS DISTINCT FROM relation_id)
		ORDER BY primary_flag DESC, complete_flag DESC, min_length 
		LIMIT 1		
	);
	
END;

$function$;

