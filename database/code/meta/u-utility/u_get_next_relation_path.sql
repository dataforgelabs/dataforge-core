
DROP FUNCTION IF EXISTS meta.u_get_next_relation_path;
CREATE OR REPLACE FUNCTION meta.u_get_next_relation_path(in_from_source_id int, in_to_source_id int, 
in_cardinality text = '1', in_start_path int[] = '{}', in_max_length int = 5)
 RETURNS json
 LANGUAGE plpgsql
AS $function$

DECLARE
	v_in_path_length int;
	v_in_path_complete boolean = false;
	v_level int;
	v_max_level int;
	v_next_relation_id int;
	v_next_complete_flag boolean;
	v_ret_selections json;
	v_ret_path jsonb := '[]';
	v_multi_path_flag boolean;
	v_next_hop_exists_flag boolean;
	v_missing_relation_ids int[];
	v_ret_relation_ids int[] := '{}';
	v_reverse_flag boolean;
BEGIN

in_start_path := COALESCE(in_start_path,'{}'::int[]);
v_in_path_length := cardinality(in_start_path);
-- check if relations in in_start_path exist and are active

in_max_length := greatest(in_max_length, v_in_path_length + 2);

SELECT array_agg(r.id) INTO v_missing_relation_ids
FROM unnest(in_start_path) r(id)
LEFT JOIN meta.source_relation sr ON sr.source_relation_id = r.id AND sr.active_flag
WHERE sr.source_relation_id IS NULL;

IF v_missing_relation_ids IS NOT NULL THEN
	RETURN json_build_object('error','Relation id(s) do not exist or are not active: ' || v_missing_relation_ids::text);
END IF;

DROP TABLE IF EXISTS _paths;
CREATE TEMP TABLE _paths ON COMMIT DROP 
AS
WITH recursive ct AS (
	SELECT ca.next_source_id, ca.cardinality,
	ARRAY[ca.source_relation_id] relation_ids, 1 path_level, ARRAY[ca.reverse_flag] reverse_flags,
	ca.one_to_one_flag, ca.source_relation_id
	FROM meta.u_relation_with_cardinality(in_from_source_id) ca
	UNION

	SELECT ca.next_source_id, ca.cardinality,
	ct.relation_ids || ca.source_relation_id relation_ids, ct.path_level + 1, ct.reverse_flags ||  ca.reverse_flag,
	ca.one_to_one_flag, ca.source_relation_id
		FROM ct CROSS JOIN meta.u_relation_with_cardinality(ct.next_source_id) ca
		WHERE ct.cardinality = '1' -- all relations prior to last one in the chain must be 1
		AND ct.path_level <= in_max_length -- + v_in_path_length
		-- prevent multiple traverse of same relation
		AND (NOT ca.source_relation_id = ANY(ct.relation_ids) -- different relation
			OR in_start_path[ct.path_level + 1] = ca.source_relation_id -- allow relation that came in via source path
			OR in_start_path[ct.path_level] = ca.source_relation_id -- allow manual chaining of repeated relations
			)
)
SELECT relation_ids,path_level, cardinality, reverse_flags
FROM ct
WHERE next_source_id = in_to_source_id; 

RAISE DEBUG 'from source_id % to source_id % _paths COUNT %', in_from_source_id, in_to_source_id, (select count(1) from _paths);

SELECT max(path_level) INTO v_max_level FROM _paths WHERE cardinality = in_cardinality;

IF v_max_level IS NULL THEN
	-- check if paths with diff catdinality exist
	IF NOT EXISTS(SELECT 1 FROM _paths) THEN
		RETURN json_build_object('error',format('No active relation paths exist from source `%s` to source `%s` with cardinality %s using start path %s',
		meta.u_get_source_name(in_from_source_id) , meta.u_get_source_name(in_to_source_id) , in_cardinality , in_start_path));
	ELSEIF in_cardinality = '1' THEN
		RETURN json_build_object('error',format('You must use aggregation for this parameter. Target source `%s`', meta.u_get_source_name(in_to_source_id) ));
	ELSEIF in_cardinality = 'M' THEN
		RETURN json_build_object('error',format('Remove aggregation from this parameter. Target source `%s`', meta.u_get_source_name(in_to_source_id) ));
	END IF;
ELSE
	DELETE FROM _paths WHERE cardinality != in_cardinality;
END IF;


FOR v_level IN 1 .. v_max_level LOOP
	-- get next hop relations
	-- auto-pick next hop: use passed selected path, don't traverse self-relation more than once
	-- prioritize primary hops, then shortest paths
		SELECT (meta.u_get_next_hop(v_ret_relation_ids, v_level, in_start_path)).*
		INTO v_next_relation_id, v_next_complete_flag, v_next_hop_exists_flag, v_reverse_flag;

		IF v_next_relation_id IS NULL THEN
			RETURN json_build_object('error', format('No relations exist for the next relation level %s. Starting path %s Current path %s',v_level,in_start_path,v_ret_relation_ids));
		END IF;

		-- get all available selections
		SELECT json_agg(sel)
		INTO v_ret_selections
		FROM (
			SELECT jsonb_build_object('relation_id', n.relation_id) || meta.u_get_relation_label(n.relation_id, n.reverse_flag) sel
			FROM _next_hop n
		) r;


	v_ret_path := v_ret_path || (jsonb_build_object('relation_id', v_next_relation_id, 'selections', v_ret_selections,'complete',v_next_complete_flag) ||  meta.u_get_relation_label(v_next_relation_id, v_reverse_flag));
	v_ret_relation_ids := v_ret_relation_ids || v_next_relation_id;

	IF v_next_complete_flag AND v_level >= v_in_path_length THEN
		IF v_next_hop_exists_flag AND v_level < v_max_level THEN
			-- pick single best reltion_id for the next selection
			SELECT (meta.u_get_next_hop(v_ret_relation_ids, v_level + 1, in_start_path)).*
			INTO v_next_relation_id, v_next_complete_flag, v_next_hop_exists_flag, v_reverse_flag;

		ELSE	
			v_next_relation_id := null;
		END IF;
		RETURN json_build_object('path',v_ret_path,'complete', true, 'relation_ids',v_ret_relation_ids, 'next', v_next_relation_id);
	END IF;
END LOOP;

RETURN json_build_object('error','Reached end of loop. Level=' || coalesce(v_level::text,'null') || '. Current relation path ' || COALESCE(v_ret_path::text,'[]'));

END;

$function$;

