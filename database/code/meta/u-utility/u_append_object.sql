-- for each key in in_object, replaces if with value form a matching key in in_add_object if it's not null. Runs recursively for jsonb keys
CREATE OR REPLACE FUNCTION meta.u_append_object(in_object jsonb, in_add_object jsonb)
    RETURNS jsonb
    LANGUAGE plpgsql
AS
$function$

DECLARE
    v_obj jsonb := in_object;
    v_key text;
    v_value jsonb;
BEGIN

    FOR v_key,v_value IN SELECT k, v FROM jsonb_each(in_object) o(k,v) WHERE in_add_object ->> o.k IS NOT NULL
        LOOP 
            IF jsonb_typeof( v_value ) = 'object' THEN 
                v_obj := jsonb_set(v_obj, ARRAY[v_key], meta.u_append_object(in_object -> v_key, in_add_object -> v_key));
            ELSE
                v_obj := jsonb_set(v_obj, ARRAY[v_key], in_add_object->v_key);
            END IF;
        END LOOP;

RETURN v_obj;
END;

$function$;