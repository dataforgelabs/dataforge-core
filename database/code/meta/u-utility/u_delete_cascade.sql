CREATE OR REPLACE FUNCTION meta.u_delete_cascade(in_id int, in_type text)
    RETURNS JSONB
    LANGUAGE plpgsql
AS
$function$

BEGIN
IF in_type = 'project' THEN
    PERFORM meta.u_delete_cascade(source_id, 'source')
    FROM meta.source WHERE project_id = in_id;

    --Delete outputs that use the connection
    PERFORM meta.u_delete_cascade(output_id, 'output')
    FROM meta.output WHERE project_id = in_id;

    DELETE FROM meta.import WHERE project_id = in_id;
    DELETE FROM meta.project WHERE project_id = in_id;

ELSEIF in_type = 'source' THEN
    --Delete enrichments recursively
   PERFORM meta.u_delete_cascade(e.enrichment_id, 'enrichment')
    FROM meta.enrichment e
    JOIN meta.enrichment_parameter ep ON e.enrichment_id = ep.parent_enrichment_id
    WHERE ep.source_id = in_id;

    PERFORM meta.u_delete_cascade(e.enrichment_id, 'enrichment')
        FROM meta.enrichment e
    WHERE e.source_id = in_id;

    --Delete relations recursively
    PERFORM meta.u_delete_cascade(sr.source_relation_id, 'relation')
    FROM meta.source_relation sr
    WHERE source_id = in_id OR related_source_id = in_id;

    --Delete output sources recursively
    PERFORM meta.u_delete_cascade(os.output_source_id, 'output_source')
    FROM meta.output_source os
    WHERE source_id = in_id;

    --Delete remaining source tables
    DELETE FROM meta.raw_attribute WHERE source_id = in_id;
    DELETE FROM meta.source WHERE source_id = in_id;


ELSEIF in_type = 'output' THEN

    --Delete output sources recursively
    PERFORM meta.u_delete_cascade(os.output_source_id, 'output_source')
    FROM meta.output_source os
    WHERE output_id = in_id;

    --Delete output related metadata
    DELETE FROM meta.output_column WHERE output_id = in_id;
    DELETE FROM meta.output WHERE output_id = in_id;


ELSEIF in_type = 'output_source' THEN

    --Delete output source related metadata
    DELETE FROM meta.output_source_column WHERE output_source_id = in_id;
    DELETE FROM meta.output_source WHERE output_source_id = in_id;

ELSEIF in_type = 'enrichment' THEN

    --Recursively delete enrichments that reference the enrichment
    PERFORM meta.u_delete_cascade(e.enrichment_id, 'enrichment')
    FROM meta.enrichment e
    JOIN meta.enrichment_parameter ep ON e.enrichment_id = ep.parent_enrichment_id
    WHERE ep.enrichment_id = in_id;

    --Recursively delete relations that reference the enrichment
    PERFORM meta.u_delete_cascade(sr.source_relation_id, 'relation')
    FROM meta.source_relation sr
    JOIN meta.source_relation_parameter srp ON sr.source_relation_id = srp.source_relation_id
    WHERE srp.enrichment_id = in_id;

    --Delete output mappings that use the enrichment
    DELETE FROM meta.output_source_column WHERE enrichment_id = in_id;

    --Delete the enrichment itself
    DELETE FROM meta.enrichment_aggregation WHERE enrichment_id = in_id;
    DELETE FROM meta.enrichment_parameter WHERE parent_enrichment_id = in_id;
    DELETE FROM meta.enrichment WHERE enrichment_id = in_id;

ELSEIF in_type = 'relation' THEN

    --Recursively delete enrichments that use the relation
    PERFORM meta.u_delete_cascade(e.enrichment_id, 'enrichment')
    FROM meta.enrichment e
    JOIN meta.enrichment_parameter ep
    ON e.enrichment_id = ep.parent_enrichment_id
    WHERE in_id = ANY(ep.source_relation_ids);

    --Delete output mappings that use the relation
    DELETE FROM meta.output_source_column WHERE in_id = ANY(source_relation_ids);

    --Delete the relation itself
    DELETE FROM meta.source_relation_parameter WHERE source_relation_id = in_id;
    DELETE FROM meta.source_relation WHERE source_relation_id = in_id;

ELSEIF in_type = 'output_columns' THEN
    PERFORM meta.u_delete_cascade(in_id, 'output_source_columns');
    DELETE FROM meta.output_column WHERE output_id = in_id;

ELSEIF in_type = 'output_source_columns' THEN
        DELETE FROM meta.output_source_column osc
    USING meta.output_column oc
    WHERE osc.output_column_id = oc.output_column_id
    AND oc.output_id = in_id;

END IF;

RETURN json_build_object('message','delete successful');


END;
$function$;