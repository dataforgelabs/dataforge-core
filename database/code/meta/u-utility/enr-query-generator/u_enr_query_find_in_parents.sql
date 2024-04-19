CREATE OR REPLACE FUNCTION meta.u_enr_query_find_in_parents(in_child_id int,
    in_parent_id int)
 RETURNS boolean -- returns true if child element has in_parent in the parental chain
 LANGUAGE plpgsql
 COST 10
AS $function$
BEGIN

RETURN EXISTS(

WITH RECURSIVE ct AS (
    SELECT e.id, e.parent_ids FROM elements e 
	WHERE e.id = in_child_id
  UNION ALL
    SELECT e.id, e.parent_ids 
	FROM elements e JOIN ct ON ct.parent_ids @> ARRAY[e.id]
)
SELECT 1 FROM ct WHERE ct.id = in_parent_id AND ct.id <> in_child_id
);
END;

$function$;