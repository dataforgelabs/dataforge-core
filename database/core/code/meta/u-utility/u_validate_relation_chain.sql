
-- validate that the relation connects 2 sources
CREATE OR REPLACE FUNCTION meta.u_validate_relation_chain(in_from_source_id int, in_to_source_id int, 
in_cardinality text, in_path int[])
 RETURNS text
 LANGUAGE plpgsql
AS $function$

DECLARE
	v_missing_relation_ids int[];
	v_max_length int = cardinality(in_path);

BEGIN

IF COALESCE(in_path,'{}'::int[]) = '{}'::int[] THEN
	RETURN 'Relation path is blank';
END IF;

-- check if relations in in_path exist and are active
SELECT array_agg(r.id) INTO v_missing_relation_ids
FROM unnest(in_path) r(id)
LEFT JOIN meta.source_relation sr ON sr.source_relation_id = r.id AND sr.active_flag
WHERE sr.source_relation_id IS NULL;

IF v_missing_relation_ids IS NOT NULL THEN
	RETURN format('Relation id(s) do not exist or are not active: %s',v_missing_relation_ids);
END IF;


IF NOT EXISTS(
	WITH recursive ct AS (
		SELECT ca.next_source_id, ca.cardinality,
		ARRAY[ca.source_relation_id] relation_ids, 1 path_level
			FROM meta.u_relation_with_cardinality(in_from_source_id) ca 
			WHERE ca.source_relation_id = in_path[1]
			
		UNION

		SELECT ca.next_source_id, ca.cardinality,
		ct.relation_ids || ca.source_relation_id relation_ids, ct.path_level + 1
			FROM ct CROSS JOIN meta.u_relation_with_cardinality(ct.next_source_id) ca
			WHERE ct.cardinality = '1' -- all relations prior to last one in the chain must be 1
			AND ca.source_relation_id = in_path[ct.path_level + 1]
			AND ct.path_level < v_max_length 
	)
	SELECT 1
	FROM ct
	WHERE next_source_id = in_to_source_id AND relation_ids = in_path AND cardinality = in_cardinality
) THEN
	RETURN format('Relation chain %s does not connect source_id=%s to source_id=%s with cardinality %s',
		in_path, in_from_source_id , in_to_source_id , in_cardinality) ;
END IF;

RETURN '';

END;

$function$;

