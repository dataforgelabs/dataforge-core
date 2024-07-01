CREATE OR REPLACE FUNCTION meta.imp_decode_relation(in_relation jsonb, in_project_id int)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$
DECLARE 
    v_ret jsonb := in_relation;
    v_decoded text[];
    v_source_id int;
    v_related_source_id int;
BEGIN
--Decode regex
v_decoded := regexp_match(in_relation->>'name', '^\[([^]]+)]-(.+)-\[([^]]+)]$');

IF v_decoded IS NULL OR v_decoded[1] IS NULL OR v_decoded[2] IS NULL OR v_decoded[3] IS NULL THEN 
    RETURN v_ret || jsonb_build_object('error', 'Invalid relation name : ' || (in_relation->>'name'));
END IF;

v_source_id :=  meta.imp_map_source(v_decoded[1], in_project_id, false);
IF v_source_id IS NULL THEN 
    RETURN v_ret || jsonb_build_object('error', format('Source `%s` does not exist', v_decoded[1]));
END IF;

v_related_source_id :=  meta.imp_map_source(v_decoded[3], in_project_id, false);
IF v_related_source_id IS NULL THEN 
    RETURN v_ret || jsonb_build_object('error', format('Source `%s` does not exist', v_decoded[3]));
END IF;


v_ret := v_ret || jsonb_build_object('source_id', v_source_id)
               || jsonb_build_object('relation_name', v_decoded[2])
               || jsonb_build_object('related_source_id', v_related_source_id);

v_decoded := regexp_match(in_relation->>'cardinality', '^(M|1)-(M|1)$');

IF v_decoded IS NULL OR v_decoded[1] IS NULL OR v_decoded[2] IS NULL THEN 
    RETURN v_ret || jsonb_build_object('error', format('Invalid cardinality %s',in_relation->>'cardinality'));
END IF;

v_ret := v_ret || jsonb_build_object('source_cardinality',v_decoded[1]) || jsonb_build_object('related_source_cardinality',v_decoded[2]);

RETURN v_ret  - 'name' - 'cardinality';

END;
$function$;