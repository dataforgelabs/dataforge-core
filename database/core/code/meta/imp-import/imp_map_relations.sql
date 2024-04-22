CREATE OR REPLACE FUNCTION meta.imp_map_relations(in_relation_uids jsonb)
    RETURNS int[]
    LANGUAGE plpgsql
AS
$function$
DECLARE 
    v_ret int[];
BEGIN
    IF in_relation_uids IS NULL THEN
        RETURN v_ret;
    END IF;
    SELECT COALESCE(array_agg(r.source_relation_id ORDER BY rn),'{}'::int[]) 
        INTO v_ret
        FROM jsonb_array_elements_text(in_relation_uids) WITH ORDINALITY a(el, rn)
        JOIN _imp_relation r ON r.relation_uid = a.el;
    -- check for unmapped
    PERFORM meta.u_assert(cardinality(v_ret) IS NOT DISTINCT FROM jsonb_array_length(in_relation_uids) ,'Unable to map relations ' || in_relation_uids::text);
    RETURN v_ret;
END;
$function$;