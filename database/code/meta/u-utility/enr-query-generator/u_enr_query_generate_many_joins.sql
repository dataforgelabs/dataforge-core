
CREATE OR REPLACE FUNCTION meta.u_enr_query_generate_many_joins(in_source_id int, in_cte int)
   
 RETURNS text
 LANGUAGE plpgsql

AS $function$
DECLARE
v_sql text := '';
v_agg_list text;
v_el meta.query_element;

BEGIN

-- Generate LATERAL LEFT JOIN in this format
-- LEFT JOIN LATERAL (SELECT avg(R.p_retailprice) A_74845_1
--       FROM dataforge.hub_2839 R WHERE T34.owner = R.p_name) J_3103_AGG ON TRUE

FOR v_el IN 
    SELECT * 
    FROM elements e
    WHERE e.cte = in_cte AND e.type IN ('many-join','sub-source-many-join') AND e.container_source_id = in_source_id
    LOOP

        RAISE DEBUG 'Adding many-join for element %', to_json(v_el);
        PERFORM meta.u_assert(v_el.expression Is NOT NULL, format('Many join %s expression is null', to_json(v_el)));

        v_agg_list := (SELECT string_agg(e.expression || ' ' || e.alias, ',') 
        FROM elements e WHERE e.type = 'many-join attribute' AND e.parent_ids @> ARRAY[v_el.id] AND e.container_source_id = in_source_id);
        PERFORM meta.u_assert( v_agg_list IS NOT NULL,'Aggregate list is NULL');


        v_sql := v_sql || E'\nLEFT JOIN LATERAL (SELECT ' || v_agg_list || 
        ' FROM ' || CASE WHEN v_el.type = 'sub-source-many-join' THEN 
            'inline(' || v_el.expression || ') R'
        ELSE
            meta.u_get_hub_table_name(v_el.source_id) || ' R WHERE ' || v_el.expression 
        END
        || ') ' || v_el.alias || ' ON true';

    END LOOP;
      

RETURN v_sql;

END;

$function$;