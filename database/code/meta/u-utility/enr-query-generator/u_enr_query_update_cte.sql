CREATE OR REPLACE FUNCTION meta.u_enr_query_update_cte(
    in_cte int, in_mode text, in_container_source_id int)
 RETURNS void
 LANGUAGE plpgsql

AS $function$
DECLARE
    v_row_count int;
    v_el meta.query_element;
    v_sqa meta.query_element;
    v_sqa_user meta.query_element;
    v_sq_attributes int[];
    v_transit meta.query_element;
    v_table_alias text = 'T' || in_container_source_id;
BEGIN

-- Add sub-source joins to outer container_source_id
--  UPDATE elements e SET cte = in_cte
--  WHERE e.type = 'sub-source-join' AND cte IS NULL AND e.container_source_id = in_container_source_id
--  AND e.parent_ids <@ (SELECT array_agg(ep.id) 
--     FROM elements ep WHERE ep.container_source_id != in_container_source_id);


-- recursively add cascading joins
LOOP
 UPDATE elements e SET cte = in_cte
 WHERE e.type IN ('join') AND cte IS NULL AND e.container_source_id = in_container_source_id
 AND e.parent_ids <@ (SELECT array_agg(ep.id) 
    FROM elements ep WHERE ep.cte <= in_cte AND ep.container_source_id = in_container_source_id
    );
 GET DIAGNOSTICS v_row_count = ROW_COUNT;

 EXIT WHEN v_row_count = 0;
END LOOP;

-- Add all transits pointing to joins we just added
 UPDATE elements e SET cte = in_cte
 WHERE e.type = 'transit' AND cte IS NULL AND e.container_source_id = in_container_source_id 
 AND e.parent_ids <@ (SELECT array_agg(ep.id) 
    FROM elements ep WHERE ep.cte <= in_cte AND ep.container_source_id = in_container_source_id
    OR (ep.type = 'sub-source-join' AND ep.source_id = e.source_id )
    );


-- Add all many-joins that can be resolved
FOR v_el IN SELECT * FROM elements e WHERE e.type IN ('many-join','sub-source-many-join') AND e.cte IS NULL AND e.container_source_id = in_container_source_id
    AND e.parent_ids <@ (SELECT array_agg(ep.id) 
    FROM elements ep WHERE ep.container_source_id = in_container_source_id AND (
        ep.cte < in_cte 
        OR (ep.type IN ('system', 'raw', 'transit') AND ep.cte = in_cte)
        OR (in_mode = 'recalculation' AND ep.type = 'enrichment' AND ep.cte = in_cte AND in_cte = 0 
            AND ep.expression = (v_table_alias || '.' || ep.alias) ) -- only for enrichments already calculated during enrichment process
        )
    )
    LOOP
    RAISE DEBUG 'Processing CTE for many-join %', to_json(v_el);

    -- check if all many-join expressions can be resolved
    IF NOT EXISTS(
        SELECT 1 FROM elements sqa 
        WHERE sqa.type = 'many-join attribute'  
        AND sqa.parent_ids @> ARRAY[v_el.id] -- all expressions
        AND sqa.container_source_id = in_container_source_id
        AND EXISTS(SELECT 1 FROM elements el WHERE el.id <> v_el.id AND sqa.parent_ids @> ARRAY[el.id] 
         AND el.cte is NULL AND el.container_source_id = in_container_source_id) 
        ) THEN 
            -- move all many-join attributes to current CTE and capture element_ids
            RAISE DEBUG 'Moving many-join % into cte %', to_json(v_el), in_cte;

            WITH cu AS (
                UPDATE elements e SET cte = in_cte WHERE e.type = 'many-join attribute'  
                AND e.parent_ids @> ARRAY[v_el.id] AND e.container_source_id = in_container_source_id
                RETURNING e.id
            ) SELECT array_agg(id) INTO v_sq_attributes
            FROM cu;
            -- move many-join to current CTE 
            UPDATE elements e 
            SET cte = in_cte
            WHERE e.id = v_el.id AND e.container_source_id = in_container_source_id;
            
    END IF;
    END LOOP;

-- Add all transits pointing to many-joins we just added
 UPDATE elements e SET cte = in_cte
 WHERE e.type = 'transit-agg' AND cte IS NULL AND e.container_source_id = in_container_source_id
 AND e.parent_ids <@ (SELECT array_agg(ep.id) FROM elements ep WHERE ep.cte <= in_cte AND ep.container_source_id = in_container_source_id);


-- Add all enrichments that can be resolved

 UPDATE elements e SET cte = in_cte
 WHERE e.type = 'enrichment' AND cte IS NULL AND e.container_source_id = in_container_source_id
 AND e.parent_ids <@ (SELECT array_agg(ep.id) FROM elements ep 
    WHERE (
            ep.container_source_id = in_container_source_id AND ( 
            ep.cte < in_cte
            OR (ep.type IN ('system', 'transit', 'transit-agg', 'raw') AND ep.cte = in_cte)
            OR (in_mode = 'recalculation' AND ep.type = 'enrichment' AND ep.cte = in_cte AND in_cte = 0)
         ))
         OR 
         (in_mode = 'sub-source' AND ep.type IN ('transit', 'transit-agg') AND cardinality(ep.container_source_ids) > 0) -- sub-source enrichment requires transit in parent source query
);



-- Short-curcuit transits in current CTE 
FOR v_el IN SELECT * FROM elements e WHERE e.type IN ('transit','transit-agg') AND e.cte = in_cte AND e.container_source_id = in_container_source_id AND NULLIF(e.container_source_ids,'{}') IS NULL
    LOOP
        RAISE DEBUG 'Short-curcuiting transit %', to_json(v_el);

        UPDATE elements e
        SET expression = regexp_replace(e.expression,'(' || v_table_alias || '\.' || v_el.alias || ')($|[^0-9])', v_el.expression || '\2','g')
        WHERE e.type in ('enrichment','many-join','many-join attribute','sub-source-many-join')
        AND e.cte = in_cte AND e.container_source_id = in_container_source_id
        AND e.expression ~ ('(' || v_table_alias || '\.' || v_el.alias || ')($|[^0-9])');
    END LOOP;



-- Remove unused transits in current CTE
DELETE FROM elements e
WHERE e.type like 'transit%' AND e.cte = in_cte AND e.container_source_id = in_container_source_id AND NULLIF(e.container_source_ids,'{}') IS NULL
AND NOT EXISTS (SELECT 1 FROM elements ce WHERE ce.cte IS NULL AND ce.parent_ids @> ARRAY[e.id] AND ce.container_source_id = in_container_source_id);

--Add cte0 to unlabeled raw and system columns, these are columns that need derived values in cte0
IF in_cte = 0 THEN
    UPDATE elements e
    SET cte = 0
    WHERE e.type IN ('raw','system') AND e.container_source_id = in_container_source_id
    AND cte IS NULL;
END IF;


END;

$function$;